package internal

import (
	"github.com/google/uuid"
	"github.com/totegamma/concurrent/x/jwt"
	"strconv"
	"time"
)

func CreateAuthToken(domain, ccid, priv string) (string, error) {
	return jwt.Create(jwt.Claims{
		JWTID:          uuid.New().String(),
		IssuedAt:       strconv.FormatInt(time.Now().Unix(), 10),
		ExpirationTime: strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10),
		Audience:       domain,
		Issuer:         ccid,
		Subject:        "concrnt",
	}, priv)
}
