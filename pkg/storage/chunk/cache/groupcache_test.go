package cache_test // _test because currently it import storage/config package which has circular dependency with `cache` package

import (
	"context"
	"testing"
	"time"

	ingesterclient "github.com/grafana/loki/pkg/ingester/client"
	"github.com/grafana/loki/pkg/logqlmodel/stats"
	"github.com/grafana/loki/pkg/storage"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/chunk/cache"
	"github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/grafana/loki/pkg/storage/chunk/client/local"
	"github.com/grafana/loki/pkg/storage/chunk/fetcher"
	"github.com/grafana/loki/pkg/storage/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	start          = model.Time(1523750400000)
	fakeOrgID      = "fake"
	testBlockSize  = 256 * 1024
	testTargetSize = 1500 * 1024
	testBlockSizes = []int{64 * 1024, 256 * 1024, 512 * 1024}
)

var labelsForDummyChunks = labels.Labels{
	{Name: labels.MetricName, Value: "foo"},
	{Name: "bar", Value: "baz"},
	{Name: "toms", Value: "code"},
}

func dummyChunk(now model.Time) chunk.Chunk {
	return dummyChunkFor(now, labelsForDummyChunks)
}

func dummyChunkFor(now model.Time, metric labels.Labels) chunk.Chunk {
	return dummyChunkForEncoding(now, metric, 1)
}

func dummyChunkForEncoding(now model.Time, metric labels.Labels, samples int) chunk.Chunk {
	c, _ := chunk.NewForEncoding(chunk.Bigchunk)
	chunkStart := now.Add(-time.Hour)

	for i := 0; i < samples; i++ {
		t := time.Duration(i) * 15 * time.Second
		nc, err := c.Add(model.SamplePair{Timestamp: chunkStart.Add(t), Value: model.SampleValue(i)})
		if err != nil {
			panic(err)
		}
		if nc != nil {
			panic("returned chunk was not nil")
		}
	}

	chunk := chunk.NewChunk(
		fakeOrgID,
		ingesterclient.Fingerprint(metric),
		metric,
		c,
		chunkStart,
		now,
	)
	// Force checksum calculation.
	err := chunk.Encode()
	if err != nil {
		panic(err)
	}
	return chunk
}

func populateChunks(t *testing.T, objCli client.Client) chunk.Chunk {
	c := dummyChunk(model.Now())
	if err := objCli.PutChunks(context.Background(), []chunk.Chunk{c}); err != nil {
		t.FailNow()
	}
	return c
}

func TestGroupcache_CacheHit(t *testing.T) {
	ctx := context.Background()
	decodedCtx := chunk.NewDecodeContext()

	client, schema := getObjectClient(t)
	gcache := cache.NewGroupcache(stats.ChunkCache, "", []string{}, fetcher.FetchChunkObjectStore(client))
	c := populateChunks(t, client)

	key := schema.ExternalKey(c.ChunkRef)

	foundKeys, chunkBytes, missedKeys, err := gcache.Fetch(ctx, []string{key})
	require.NoError(t, err)
	assert.Equal(t, len(foundKeys), 1)
	assert.Equal(t, len(missedKeys), 0)
	assert.Equal(t, len(chunkBytes), 1)

	c2 := chunk.Chunk{
		ChunkRef: c.ChunkRef,
	}

	require.NoError(t, c2.Decode(decodedCtx, chunkBytes[0]))

	c2.Encode()

	assert.Equal(t, c.Checksum, c2.Checksum)
}

type testObjectClient struct {
	client.ObjectClient
	path string
}

func getObjectClient(t *testing.T) (client.Client, config.SchemaConfig) {
	t.Helper()

	path := "/tmp/test-loki/"

	schemaConfig := config.SchemaConfig{
		Configs: []config.PeriodConfig{
			{
				From:       config.DayTime{Time: start},
				IndexType:  "boltdb",
				ObjectType: "filesystem",
				Schema:     "v9",
				IndexTables: config.PeriodicTableConfig{
					Prefix: "index_",
					Period: time.Hour * 168,
				},
			},
		},
	}

	c, err := storage.NewObjectClient("filesystem", storage.Config{
		FSConfig: local.FSConfig{
			Directory: path,
		},
	}, storage.NewClientMetrics())
	if err != nil {
		panic(err)
	}
	return client.NewClient(&testObjectClient{
		ObjectClient: c,
		path:         path,
	}, client.FSEncoder, schemaConfig), schemaConfig

	// // limits, err := validation.NewOverrides(validation.Limits{
	// // 	MaxQueryLength: model.Duration(6000 * time.Hour),
	// // }, nil)
	// // if err != nil {
	// // 	panic(err)
	// // }

	// storeConfig := storage.Config{
	// 	BoltDBConfig:      local.BoltDBConfig{Directory: "/tmp/benchmark/index"},
	// 	FSConfig:          local.FSConfig{Directory: "/tmp/benchmark/chunks"},
	// 	MaxChunkBatchSize: 10,
	// }

	// c, err := storage.NewChunkClient("filesystem", storeConfig, schemaConfig, storage.NewClientMetrics(), nil)

	// if err != nil {
	// 	panic(err)
	// }

	// return c
}
