package fetcher

import (
	"github.com/mailgun/groupcache/v2"
)

type Groupcache struct {
	pool   groupcache.HTTPPool
	chunks *groupcache.Group
}

func NewGroupcache(me string, peers []string) *Groupcache {
	return nil
}
