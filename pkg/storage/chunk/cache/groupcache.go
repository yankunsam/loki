package cache

import (
	"context"

	"github.com/grafana/loki/pkg/logqlmodel/stats"
	"github.com/mailgun/groupcache/v2"
	"go.uber.org/multierr"
)

type Groupcache struct {
	pool      *groupcache.HTTPPool
	group     *groupcache.Group
	cacheType stats.CacheType
}

type GetChunkFunc = groupcache.GetterFunc

const (
	maxSize = 30 * (1 << 20) // 30MB
)

func NewGroupcache(
	name stats.CacheType,
	me string,
	peers []string,
	gf GetChunkFunc,
) *Groupcache {
	return &Groupcache{
		pool:      groupcache.NewHTTPPool(me),
		group:     groupcache.NewGroup(string(name), maxSize, gf),
		cacheType: name,
	}
}

func (c *Groupcache) Store(ctx context.Context, key []string, buf [][]byte) error {
	panic("unimplemented")
}

// Fetch makes sure, order in returned [][]bytes is same as in keys.
func (c *Groupcache) Fetch(ctx context.Context, keys []string) ([]string, [][]byte, []string, error) {
	found := make([]string, 0)
	errs := make([]error, 0)
	chunks := make([][]byte, 0, 0)

	for _, key := range keys {
		b := make([]byte, 0, 1024) // TODO(kavi): how do we pre-allocate? is it needed?
		if err := c.group.Get(ctx, key, groupcache.AllocatingByteSliceSink(&b)); err != nil {
			errs = append(errs, err)
		}
		chunks = append(chunks, b)
		found = append(found, key)
	}

	return found, chunks, nil, multierr.Combine(errs...)
}

func (c *Groupcache) Stop() {

}

func (c *Groupcache) GetCacheType() stats.CacheType {
	return c.cacheType
}
