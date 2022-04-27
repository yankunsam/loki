package util

import (
	"bytes"
	"context"
	"sync"

	"github.com/grafana/loki/pkg/storage/stores/series/index"
	util_math "github.com/grafana/loki/pkg/util/math"
)

const maxQueriesPerGoroutine = 100

type TableQuerier interface {
	MultiQueries(ctx context.Context, queries []index.Query, callback index.QueryPagesCallback) error
}

// QueriesByTable groups and returns queries by tables.
func QueriesByTable(queries []index.Query) map[string][]index.Query {
	queriesByTable := make(map[string][]index.Query)
	for _, query := range queries {
		if _, ok := queriesByTable[query.TableName]; !ok {
			queriesByTable[query.TableName] = []index.Query{}
		}

		queriesByTable[query.TableName] = append(queriesByTable[query.TableName], query)
	}

	return queriesByTable
}

func DoParallelQueries(ctx context.Context, tableQuerier TableQuerier, queries []index.Query, callback index.QueryPagesCallback) error {
	if len(queries) == 0 {
		return nil
	}
	errs := make(chan error)

	if len(queries) <= maxQueriesPerGoroutine {
		return tableQuerier.MultiQueries(ctx, queries, NewCallbackDeduper(callback))
	}

	callback = NewSyncCallbackDeduper(callback)
	for i := 0; i < len(queries); i += maxQueriesPerGoroutine {
		q := queries[i:util_math.Min(i+maxQueriesPerGoroutine, len(queries))]
		go func(queries []index.Query) {
			errs <- tableQuerier.MultiQueries(ctx, queries, callback)
		}(q)
	}

	var lastErr error
	for i := 0; i < len(queries); i += maxQueriesPerGoroutine {
		err := <-errs
		if err != nil {
			lastErr = err
		}
	}

	return lastErr
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// NewCallbackDeduper should always be used on table level not the whole query level because it just looks at range values which can be repeated across tables
// Cortex anyways dedupes entries across tables
func NewSyncCallbackDeduper(callback index.QueryPagesCallback) index.QueryPagesCallback {
	seen := sync.Map{}
	return func(q index.Query, rbr index.ReadBatchResult) bool {
		return callback(q, &filteringBatch{
			ReadBatchIterator: rbr.Iterator(),
			seen: func(rangeValue []byte) bool {
				buf := bufferPool.Get().(*bytes.Buffer)
				defer bufferPool.Put(buf)
				buf.Reset()
				buf.WriteString(q.HashValue)
				buf.Write(rangeValue)
				if _, loaded := seen.Load(GetUnsafeString(buf.Bytes())); loaded {
					return true
				}
				seen.Store(buf.String(), struct{}{})
				return false
			},
		})
	}
}

func NewCallbackDeduper(callback index.QueryPagesCallback) index.QueryPagesCallback {
	return func(q index.Query, rbr index.ReadBatchResult) bool {
		seen := map[string]struct{}{}
		return callback(q, &filteringBatch{
			ReadBatchIterator: rbr.Iterator(),
			seen: func(rangeValue []byte) bool {
				h := GetUnsafeString(rangeValue)
				if _, loaded := seen[h]; loaded {
					return true
				}
				seen[h] = struct{}{}
				return false
			},
		})
	}
}

type filteringBatch struct {
	index.ReadBatchIterator

	seen func([]byte) bool
}

func (f *filteringBatch) Iterator() index.ReadBatchIterator {
	return f
}

func (f *filteringBatch) Next() bool {
	for f.ReadBatchIterator.Next() {
		if f.seen(f.RangeValue()) {
			continue
		}
		return true
	}

	return false
}
