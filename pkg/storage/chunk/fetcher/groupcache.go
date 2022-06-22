package fetcher

import (
	"context"
	"time"

	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/chunk/cache"
	"github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/mailgun/groupcache/v2"
)

type Groupcache struct {
	pool   *groupcache.HTTPPool
	group  *groupcache.Group
	source client.Client
}

const (
	maxSize = 30 * (1 << 20) // 30MB
)

func NewGroupcache(name string, me string, peers []string, source client.Client) *Groupcache {
	return &Groupcache{
		pool: groupcache.NewHTTPPool(me),
		group: groupcache.NewGroup(name, maxSize, groupcache.GetterFunc(
			func(ctx context.Context, id string, dest groupcache.Sink) error {

				chunk, err := fetchChunk(ctx, id)
				if err != nil {
					return err
				}

				b, err := chunk.Encoded()
				if err != nil {
					return err
				}

				// Set the chunk in the groupcache to expire after 5 minutes
				return dest.SetBytes(b, time.Now().Add(time.Minute*5))
			},
		)),
		source: source,
	}
}

func (c *Groupcache) FetchChunks(ctx context.Context, chunks []chunk.Chunk, keys []string) ([]chunk.Chunk, error) {
	return nil, nil
}

func (c *Groupcache) WriteBackCache(ctx context.Context, chunks []chunk.Chunk) error {
	return nil
}

func (c *Groupcache) Stop() {

}

func (c *Groupcache) Cache() cache.Cache {
	return nil
}

func (c *Groupcache) Client() client.Client {
	return c.source
}

func (c *Groupcache) IsChunkNotFoundErr(err error) bool {
	return c.source.IsChunkNotFoundErr(err)
}

func fetchChunk(ctx context.Context, id string) (*chunk.Chunk, error) {
	return &chunk.Chunk{}, nil
}
