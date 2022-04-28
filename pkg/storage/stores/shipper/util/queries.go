package util

import (
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

// NewCallbackDeduper should always be used on table level not the whole query level because it just looks at range values which can be repeated across tables
// Cortex anyways dedupes entries across tables
func NewSyncCallbackDeduper(callback index.QueryPagesCallback) index.QueryPagesCallback {
	seen := sync.Map{}
	return func(q index.Query, rbr index.ReadBatchResult) bool {
		return callback(q, &filteringBatch{
			ReadBatchIterator: rbr.Iterator(),
			seen: func(rangeValue []byte) bool {
				any, ok := seen.Load(q.HashValue)
				if !ok {
					hashes := &sync.Map{}
					hashes.Store(GetUnsafeString(rangeValue), struct{}{})
					seen.Store(q.HashValue, hashes)
					return false
				}
				hashes := any.(*sync.Map)
				_, loaded := hashes.LoadOrStore(GetUnsafeString(rangeValue), struct{}{})
				return loaded
			},
		})
	}
}

func NewCallbackDeduper(callback index.QueryPagesCallback) index.QueryPagesCallback {
	f := &filter{
		seen: map[string]map[string]struct{}{},
	}
	return func(q index.Query, rbr index.ReadBatchResult) bool {
		f.hashValue = q.HashValue
		f.ReadBatchIterator = rbr.Iterator()
		return callback(q, f)
	}
}

type filter struct {
	index.ReadBatchIterator
	hashValue string
	seen      map[string]map[string]struct{}
}

func (f *filter) Iterator() index.ReadBatchIterator {
	return f
}

func (f *filter) Next() bool {
	for f.ReadBatchIterator.Next() {
		rangeValue := f.RangeValue()
		hashes, ok := f.seen[f.hashValue]
		if !ok {
			hashes = map[string]struct{}{}
			hashes[GetUnsafeString(rangeValue)] = struct{}{}
			f.seen[f.hashValue] = hashes
			return true
		}
		h := GetUnsafeString(rangeValue)
		if _, loaded := hashes[h]; loaded {
			continue
		}
		hashes[h] = struct{}{}
		return true
	}

	return false
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
