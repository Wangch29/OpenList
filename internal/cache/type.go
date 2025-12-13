package cache

import "time"

type Expirable interface {
	Expired() bool
}

type ExpirationTime time.Time

func (e ExpirationTime) Expired() bool {
	return time.Now().After(time.Time(e))
}

type NoExpiration struct{}

func (NoExpiration) Expired() bool {
	return false
}

type CacheEntry[T any] struct {
	Expirable
	data T
}
