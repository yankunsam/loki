package fetcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/chunk/cache"
	"github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/grafana/loki/pkg/storage/config"
	"github.com/mailgun/groupcache/v2"
)

type ReadThrough struct {
	source client.Client
	cache  *cache.Groupcache
	logger log.Logger
	schema config.SchemaConfig
}

const (
	maxSize = 30 * (1 << 20) // 30MB
)

func NewReadThrough(logger log.Logger, schema config.SchemaConfig, cache *cache.Groupcache, source client.Client) *ReadThrough {
	return &ReadThrough{
		source: source,
		cache:  cache,
		logger: logger,
		schema: schema,
	}
}

func (c *ReadThrough) FetchChunks(ctx context.Context, chunks []chunk.Chunk, keys []string) ([]chunk.Chunk, error) {
	errs := make([]error, 0)
	result := make([]chunk.Chunk, 0)

	foundKeys, chunksGot, missingKeys, err := c.cache.Fetch(ctx, keys)
	if len(foundKeys) > 0 {
		level.Info(c.logger).Log("found keys", len(foundKeys))
	}
	if len(missingKeys) > 0 {
		level.Info(c.logger).Log("missing keys", len(missingKeys))
	}

	decodeContext := chunk.NewDecodeContext()

	// TODO(kavi): Will order of chunks same in []keys and []chunk.Chunk?. Otherwise below loop is going to break.
	i, j := 0, 0
	for i < len(chunks) && j < len(foundKeys) {
		chunkKey := c.schema.ExternalKey(chunks[i].ChunkRef)

		if chunkKey < foundKeys[i] {
			i++
			continue
		}
		if chunkKey > foundKeys[i] {
			j++
			continue
		}

		if err := chunks[i].Decode(decodeContext, chunksGot[j]); err != nil {
			errs = append(errs, err)
		}

		result = append(result, chunks[i])
		i++
		j++
	}

	return result, err
}

func (c *ReadThrough) WriteBackCache(ctx context.Context, chunks []chunk.Chunk) error {
	return nil
}

func (c *ReadThrough) Stop() {

}

func (c *ReadThrough) Cache() cache.Cache {
	return c.cache
}

func (c *ReadThrough) Client() client.Client {
	return c.source
}

func (c *ReadThrough) IsChunkNotFoundErr(err error) bool {
	return c.source.IsChunkNotFoundErr(err)
}

func FetchChunkObjectStore(source client.Client) groupcache.GetterFunc {
	return func(ctx context.Context, id string, dst groupcache.Sink) error {
		chnk, err := chunk.ParseExternalKeyWithoutUserID(id)
		if err != nil {
			return err
		}
		chunks, err := source.GetChunks(ctx, []chunk.Chunk{chnk})
		if err != nil {
			return err
		}

		if len(chunks) <= 0 {
			return errors.New("empty chunks returned from storage")
		}

		if len(chunks) > 1 {
			fmt.Println("got chunks, more than asked for")
		}

		b, err := chunks[0].Encoded()
		if err != nil {
			return err
		}

		if err := dst.SetBytes(b, time.Now().Add(time.Minute*5)); err != nil {
			return err
		}

		return nil
	}

}
