package worker

import (
	"context"
	"encoding/json"
	"github.com/concrnt/ccworld-ap-bridge/internal"
	"github.com/totegamma/concurrent/client"
	"log"
	"slices"
	"time"

	"github.com/totegamma/concurrent/core"

	"github.com/concrnt/ccworld-ap-bridge/types"
	"github.com/concrnt/ccworld-ap-bridge/world"
)

type DeliverState struct {
	Dests   []string
	Listens []string
}

func (d DeliverState) Equals(other DeliverState) bool {
	if len(d.Dests) != len(other.Dests) {
		log.Printf("[DEBUG] DeliverState.Equals: length of Dests mismatch: %d vs %d", len(d.Dests), len(other.Dests))
		return false
	}

	for _, dest := range d.Dests {
		if !slices.Contains(other.Dests, dest) {
			log.Printf("[DEBUG] DeliverState.Equals: not found dest %s in other.Dests", dest)
			return false
		}
	}

	if len(d.Listens) != len(other.Listens) {
		log.Printf("[DEBUG] DeliverState.Equals: length of Listens mismatch: %d vs %d", len(d.Listens), len(other.Listens))
		return false
	}

	for _, listen := range d.Listens {
		if !slices.Contains(other.Listens, listen) {
			log.Printf("[DEBUG] DeliverState.Equals: not found listen %s in other.Listens", listen)
			return false
		}
	}

	return true
}

func (w *Worker) StartMessageWorker() {

	log.Printf("start message worker")

	ticker10 := time.NewTicker(10 * time.Second)
	workers := make(map[string]context.CancelFunc)
	states := make(map[string]DeliverState)

	for ; true; <-ticker10.C {
		log.Printf("[DEBUG] =========== Ticker triggered, start iteration ===========")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// 1. GetAllFollowers
		log.Printf("[DEBUG] calling store.GetAllFollowers...")
		followers, err := w.store.GetAllFollowers(ctx)
		if err != nil {
			log.Printf("worker/message GetAllFollowers: %v", err)
		} else {
			log.Printf("[DEBUG] fetched %d followers from GetAllFollowers", len(followers))
		}

		delivers := make(map[string][]types.ApFollower)
		for i, job := range followers {
			log.Printf("[DEBUG] follower[%d]: PublisherUserID=%s, SubscriberInbox=%s", i, job.PublisherUserID, job.SubscriberInbox)
			if _, ok := delivers[job.PublisherUserID]; !ok {
				delivers[job.PublisherUserID] = make([]types.ApFollower, 0)
			}
			delivers[job.PublisherUserID] = append(delivers[job.PublisherUserID], job)
		}

		log.Printf("[DEBUG] grouping followers by PublisherUserID -> got %d groups", len(delivers))

		// 2. For each publisher user, prepare to deliver
		for userID, deliver := range delivers {
			log.Printf("[DEBUG] Processing userID=%s with %d deliver jobs", userID, len(deliver))

			mustRestart := false
			existingWorker, workerFound := workers[userID]
			existingState, stateFound := states[userID]

			if !workerFound {
				log.Printf("[DEBUG] no existing worker found for userID=%s", userID)
			}
			if !stateFound {
				log.Printf("[DEBUG] no existing state found for userID=%s", userID)
			}
			if !workerFound || !stateFound {
				mustRestart = true
			}

			// 3. Get user entity
			log.Printf("[DEBUG] calling store.GetEntityByID for userID=%s ...", userID)
			entity, err := w.store.GetEntityByID(ctx, userID)
			if err != nil {
				log.Printf("worker/message/%v GetEntityByID: %v", userID, err)
				continue
			}
			log.Printf("[DEBUG] store.GetEntityByID returned entity.CCID=%s", entity.CCID)
			ownerID := entity.CCID

			// 4. Build new DeliverState
			var newState DeliverState
			listenTimelines := make([]string, 0)
			log.Printf("[DEBUG] calling store.GetUserSettings for ownerID=%s ...", ownerID)
			userSettings, err := w.store.GetUserSettings(ctx, entity.CCID)
			if err != nil {
				log.Printf("[DEBUG] GetUserSettings failed for ownerID=%s, err=%v", ownerID, err)
			} else {
				log.Printf("[DEBUG] userSettings.ListenTimelines = %v", userSettings.ListenTimelines)
				listenTimelines = append(listenTimelines, userSettings.ListenTimelines...)
			}

			if len(listenTimelines) == 0 {
				log.Printf("[DEBUG] no userSettings.ListenTimelines found, fallback to UserHomeStream for ownerID=%s", ownerID)
				listenTimelines = append(listenTimelines, world.UserHomeStream+"@"+ownerID)
			}

			newState.Dests = make([]string, 0)
			newState.Listens = listenTimelines

			for _, job := range deliver {
				newState.Dests = append(newState.Dests, job.SubscriberInbox)
			}
			log.Printf("[DEBUG] newState.Dests=%v", newState.Dests)
			log.Printf("[DEBUG] newState.Listens=%v", newState.Listens)

			// 5. Compare with existing state
			if workerFound && stateFound {
				log.Printf("[DEBUG] existingState for userID=%s => Dests=%v, Listens=%v", userID, existingState.Dests, existingState.Listens)
			}
			if !newState.Equals(existingState) {
				log.Printf("[DEBUG] newState != existingState, mustRestart=true for userID=%s", userID)
				mustRestart = true
			}

			// 6. Restart worker if needed
			if mustRestart {
				if workerFound {
					log.Printf("worker/message/%v cancel worker\n", userID)
					existingWorker() // cancel existing context
				}

				log.Printf("worker/message/%v start worker \n", userID)

				timelines := make([]string, 0)
				for _, listenTimeline := range newState.Listens {
					log.Printf("[DEBUG] calling client.GetTimeline for listenTimeline=%s ...", listenTimeline)
					timeline, err := w.client.GetTimeline(ctx, w.config.FQDN, listenTimeline, nil)
					if err != nil {
						log.Printf("worker/message/%v GetTimeline: %v", userID, err)
						continue
					}

					log.Printf("[DEBUG] timeline.ID = %s -> appended as '%s@%s'", timeline.ID, timeline.ID, w.config.FQDN)
					timelines = append(timelines, timeline.ID+"@"+w.config.FQDN)
				}

				if len(timelines) == 0 {
					log.Printf("worker/message/%v no timelines to listen", userID)
					continue
				}

				// 7. Subscribe to Redis
				log.Printf("[DEBUG] creating pubsub...")
				pubsub := w.rdb.Subscribe(ctx)
				log.Printf("[DEBUG] pubsub.Subscribe to %v", timelines)
				err := pubsub.Subscribe(ctx, timelines...)
				if err != nil {
					log.Printf("worker/message/%v pubsub.Subscribe %v", userID, err)
					continue
				}

				workerctx, cancel := context.WithCancel(context.Background())
				workers[userID] = cancel
				states[userID] = newState

				// 8. Start worker goroutine
				go func(ctx context.Context, publisherUserID string, subscriberInboxes []string) {
					log.Printf("[DEBUG] -> started goroutine for userID=%s", publisherUserID)
					defer func() {
						log.Printf("[DEBUG] -> goroutine for userID=%s is exiting...", publisherUserID)
					}()

					for {
						select {
						case <-ctx.Done():
							log.Printf("[DEBUG] context canceled for userID=%s", publisherUserID)
							delete(workers, publisherUserID)
							delete(states, publisherUserID)
							return
						default:
							pubsubMsg, err := pubsub.ReceiveMessage(ctx)
							if ctx.Err() != nil {
								log.Printf("[DEBUG] ctx.Err() != nil for userID=%s, exit goroutine", publisherUserID)
								delete(workers, publisherUserID)
								delete(states, publisherUserID)
								return
							}
							if err != nil {
								log.Printf("worker/message/%v pubsub.ReceiveMessage %v", publisherUserID, err)
								delete(workers, publisherUserID)
								delete(states, publisherUserID)
								return
							}

							// [DEBUG] logging the raw message payload
							log.Printf("[DEBUG] userID=%s received pubsubMsg.Payload=%q", publisherUserID, pubsubMsg.Payload)

							// Check if payload is empty or invalid JSON
							if pubsubMsg.Payload == "" {
								log.Printf("[DEBUG] userID=%s empty payload, skip", publisherUserID)
								continue
							}
							if !json.Valid([]byte(pubsubMsg.Payload)) {
								log.Printf("[DEBUG] userID=%s invalid JSON payload: %s", publisherUserID, pubsubMsg.Payload)
								continue
							}

							var streamEvent core.Event
							err = json.Unmarshal([]byte(pubsubMsg.Payload), &streamEvent)
							if err != nil {
								log.Printf("worker/message/%v json.Unmarshal streamEvent %v", publisherUserID, err)
								continue
							}

							log.Printf("[DEBUG] userID=%s unmarshal core.Event => Type=%s, Document=%s", publisherUserID, streamEvent.Type, streamEvent.Document)

							if streamEvent.Document == "" {
								log.Printf("[DEBUG] userID=%s empty streamEvent.Document, skip", publisherUserID)
								continue
							}

							var document core.DocumentBase[any]
							err = json.Unmarshal([]byte(streamEvent.Document), &document)
							if err != nil {
								log.Printf("worker/message/%v json.Unmarshal document %v", publisherUserID, err)
								continue
							}

							log.Printf("[DEBUG] userID=%s document unmarshaled => Type=%s, Signer=%s", publisherUserID, document.Type, document.Signer)

							// only process if signer == ownerID
							if document.Signer != ownerID {
								log.Printf("[DEBUG] userID=%s document.Signer=%s != ownerID=%s, skip", publisherUserID, document.Signer, ownerID)
								continue
							}

							// 9. Create token
							log.Printf("[DEBUG] calling CreateAuthToken for userID=%s ...", publisherUserID)
							token, err := internal.CreateAuthToken(w.config.FQDN, w.config.ProxyCCID, w.config.ProxyPriv)
							if err != nil {
								log.Printf("worker/message/%v CreateAuthToken %v", publisherUserID, err)
								continue
							}

							var object *types.ApObject

							switch document.Type {
							case "message":
								log.Printf("[DEBUG] userID=%s document.Type=message => converting to Note or Announce...", publisherUserID)
								messageID := streamEvent.Item.ResourceID
								note, err := w.bridge.MessageToNote(ctx, messageID, &client.Options{
									AuthToken: token,
								})
								if err != nil {
									log.Printf("worker/message/%v MessageToNote %v", publisherUserID, err)
									continue
								}

								if note.Type == "Announce" {
									announce := types.ApObject{
										Context: []string{"https://www.w3.org/ns/activitystreams"},
										Type:    "Announce",
										ID:      "https://" + w.config.FQDN + "/ap/note/" + messageID + "/activity",
										Actor:   "https://" + w.config.FQDN + "/ap/acct/" + publisherUserID,
										Content: "",
										Object:  note.Object,
										To:      note.To,
									}
									object = &announce
								} else {
									create := types.ApObject{
										Context: []string{"https://www.w3.org/ns/activitystreams"},
										Type:    "Create",
										ID:      "https://" + w.config.FQDN + "/ap/note/" + messageID + "/activity",
										Actor:   "https://" + w.config.FQDN + "/ap/acct/" + publisherUserID,
										To:      note.To,
										Object:  note,
									}
									object = &create
								}

							case "delete":
								log.Printf("[DEBUG] userID=%s document.Type=delete => converting to Tombstone...", publisherUserID)
								var deleteDoc core.DeleteDocument
								err = json.Unmarshal([]byte(streamEvent.Document), &deleteDoc)
								if err != nil {
									log.Printf("worker/message/%v json.Unmarshal deleteDoc %v", publisherUserID, err)
									continue
								}

								if len(deleteDoc.Target) == 0 {
									log.Printf("[DEBUG] userID=%s deleteDoc.Target is empty, skip", publisherUserID)
									continue
								}
								if deleteDoc.Target[0] != 'm' {
									log.Printf("[DEBUG] userID=%s deleteDoc.Target=%s is not message (not 'm'), skip", publisherUserID, deleteDoc.Target)
									continue
								}

								deleteObj := types.ApObject{
									Context: "https://www.w3.org/ns/activitystreams",
									Type:    "Delete",
									ID:      "https://" + w.config.FQDN + "/ap/note/" + deleteDoc.Target + "/delete",
									Actor:   "https://" + w.config.FQDN + "/ap/acct/" + publisherUserID,
									Object: types.ApObject{
										Type: "Tombstone",
										ID:   "https://" + w.config.FQDN + "/ap/note/" + deleteDoc.Target,
									},
								}
								object = &deleteObj

							default:
								log.Printf("[DEBUG] userID=%s document.Type=%s not supported, skip", publisherUserID, document.Type)
								continue
							}

							// 10. Post to each subscriber inbox
							if object != nil {
								for _, subscriberInbox := range subscriberInboxes {
									log.Printf("[DEBUG] userID=%s posting to subscriberInbox=%s objectType=%s", publisherUserID, subscriberInbox, object.Type)
									err = w.apclient.PostToInbox(ctx, subscriberInbox, *object, entity)
									if err != nil {
										log.Printf("worker/message/%v PostToInbox %v %v", publisherUserID, subscriberInbox, err)
										continue
									}
								}
							}
						}
					}
				}(workerctx, userID, newState.Dests)
			}
		}

		// 11. create job id list
		var validUsers []string
		for userID := range workers {
			validUsers = append(validUsers, userID)
		}
		log.Printf("[DEBUG] validUsers from workers map: %v", validUsers)

		// 12. Clean up any worker that isn't valid
		for routineID, cancel := range workers {
			if !slices.Contains(validUsers, routineID) {
				log.Printf("worker/message/%v cancel worker\n", routineID)
				cancel()
				delete(workers, routineID)
			}
		}

		log.Printf("[DEBUG] =========== Ticker iteration end ===========")
	}
}
