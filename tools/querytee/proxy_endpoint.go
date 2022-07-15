package querytee

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/websocket"
	util_log "github.com/grafana/loki/pkg/util/log"
)

type ResponsesComparator interface {
	Compare(expected, actual []byte) error
}

type ProxyEndpoint struct {
	backends   []*ProxyBackend
	metrics    *ProxyMetrics
	logger     log.Logger
	comparator ResponsesComparator

	// Whether for this endpoint there's a preferred backend configured.
	hasPreferredBackend bool

	// The route name used to track metrics.
	routeName string

	// Whether to use websockets
	useWebsockets bool
}

func NewProxyEndpoint(backends []*ProxyBackend, routeName string, metrics *ProxyMetrics, logger log.Logger, comparator ResponsesComparator, useWebsockets bool) *ProxyEndpoint {
	hasPreferredBackend := false
	for _, backend := range backends {
		if backend.preferred {
			hasPreferredBackend = true
			break
		}
	}

	return &ProxyEndpoint{
		backends:            backends,
		routeName:           routeName,
		metrics:             metrics,
		logger:              logger,
		comparator:          comparator,
		hasPreferredBackend: hasPreferredBackend,
		useWebsockets:       useWebsockets,
	}
}

func (p *ProxyEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Send the same request to all backends.
	resCh := make(chan *backendResponse, len(p.backends))

	if p.useWebsockets {
		p.executeBackendRequestsWebSockets(w, r)
		return
	}

	go p.executeBackendRequests(r, resCh)

	// Wait for the first response that's feasible to be sent back to the client.
	downstreamRes := p.waitBackendResponseForDownstream(resCh)

	if downstreamRes.err != nil {
		http.Error(w, downstreamRes.err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(downstreamRes.status)
		if _, err := w.Write(downstreamRes.body); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to write response", "err", err)
		}
	}

	p.metrics.responsesTotal.WithLabelValues(downstreamRes.backend.name, r.Method, p.routeName).Inc()
}

func (p *ProxyEndpoint) executeBackendRequestsWebSockets(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	level.Debug(p.logger).Log("msg", "Received websocket request", "path", r.URL.Path, "query", r.URL.RawQuery)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		level.Error(p.logger).Log("msg", "Unable to upgrade websocket connection", "err", err)
		return
	}

	level.Debug(p.logger).Log("msg", "Websocket connection upgraded", "path", r.URL.Path, "query", r.URL.RawQuery)

	// Setup websockets on all backends
	backendConnections := make([]*websocket.Conn, len(p.backends), len(p.backends))
	for i, backend := range p.backends {
		// TODO: Maybe it makes sense to have a Proxy interface and then spezialize an HTTP and a Websockets proxy
		url := *r.URL
		url.Host = backend.endpoint.Host
		url.Scheme = "ws"
		url.Path = path.Join(backend.endpoint.Path, r.URL.Path)
		url.RawQuery = r.URL.RawQuery

		level.Debug(p.logger).Log("Dialing backend", "url", url.String())

		backendConn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
		if err != nil {
			level.Error(p.logger).Log("msg", "Unable to setup websocket", "backend", backend.name, "err", err)
			continue
		}

		backendConnections[i] = backendConn
	}

	level.Debug(p.logger).Log("msg", "Dialed to all backends", "path", r.URL.Path, "query", r.URL.RawQuery)

	// A working group for reads and writes
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Read Messages from client and forward them to backends
	go func() {
		defer wg.Done()

		for {
			level.Debug(p.logger).Log("msg", "Waiting for message from client")

			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				level.Error(p.logger).Log("msg", "Unable to read websocket msg from client", "err", err)
				break
			}

			level.Debug(p.logger).Log("msg", "Received websocket msg from client", "msgType", msgType, "msg", string(msg))

			// TODO: Make this aync
			for i, backend := range p.backends {
				backendConn := backendConnections[i]
				if backendConn == nil {
					level.Warn(p.logger).Log("msg", "Backend websocket connection is nil. Skipping.", "backend", backend.name)
					continue
				}

				level.Debug(p.logger).Log("msg", "Forwarding message to backend", "backend", backend.name, "msgType", msgType, "msg", string(msg))

				if err := backendConn.WriteMessage(msgType, msg); err != nil {
					level.Error(p.logger).Log("msg", "Unable to write message", "err", err)
					break
				}

				level.Debug(p.logger).Log("msg", "Message forwarded to backend", "backend", backend.name, "msgType", msgType, "msg", string(msg))
			}

			level.Debug(p.logger).Log("msg", "Sent websocket msg to backends", "msgType", msgType, "msg", string(msg))
		}
	}()

	// Read from preferred backend and forward to client
	// TODO: If there is no preferred, use first backend to answer.
	// TODO: Read from all the backends async
	go func() {
		defer wg.Done()

		// Look for the preferred backend othersise use the first one
		preferredBackendIdx := 0
		for i, backend := range p.backends {
			if backend.preferred {
				preferredBackendIdx = i
			}
		}

		for {
			backendConn := backendConnections[preferredBackendIdx]
			if backendConn == nil {
				level.Warn(p.logger).Log("msg", "Backend websocket connection is nil. Skipping.", "backend", p.backends[preferredBackendIdx].name)
				break
			}

			level.Debug(p.logger).Log("msg", "Waiting for message from backend", "backend", p.backends[preferredBackendIdx].name)

			msgType, msg, err := backendConn.ReadMessage()
			if err != nil {
				level.Error(p.logger).Log("msg", "Unable to read message from backend", "err", err)
				break
			}

			level.Debug(p.logger).Log("msg", "Received websocket msg from backend", "msgType", msgType, "msg", string(msg))

			if err := conn.WriteMessage(msgType, msg); err != nil {
				level.Error(p.logger).Log("msg", "Unable to write message back to client", "err", err)
				break
			}

			level.Debug(p.logger).Log("msg", "Message forwarded to client", "msgType", msgType, "msg", string(msg))
		}
	}()

	// Wait for both reads and writes to finish
	wg.Wait()
	for i, backendConn := range backendConnections {
		if backendConn != nil {
			if err := backendConn.Close(); err != nil {
				level.Error(p.logger).Log("msg", "Unable to close websocket connection", "err", err, "backend", p.backends[i].name)
			}
		}
	}
}

func (p *ProxyEndpoint) executeBackendRequests(r *http.Request, resCh chan *backendResponse) {
	var (
		wg           = sync.WaitGroup{}
		err          error
		body         []byte
		responses    = make([]*backendResponse, 0, len(p.backends))
		responsesMtx = sync.Mutex{}
		query        = r.URL.RawQuery
	)

	if r.Body != nil {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			level.Warn(p.logger).Log("msg", "Unable to read request body", "err", err)
			return
		}
		if err := r.Body.Close(); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to close request body", "err", err)
		}

		r.Body = ioutil.NopCloser(bytes.NewReader(body))
		if err := r.ParseForm(); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to parse form", "err", err)
		}
		query = r.Form.Encode()
	}

	level.Debug(p.logger).Log("msg", "Received request", "path", r.URL.Path, "query", query)

	wg.Add(len(p.backends))
	for _, b := range p.backends {
		b := b

		go func() {
			defer wg.Done()
			var (
				bodyReader io.ReadCloser
				start      = time.Now()
			)
			if len(body) > 0 {
				bodyReader = ioutil.NopCloser(bytes.NewReader(body))
			}

			status, body, err := b.ForwardRequest(r, bodyReader)
			elapsed := time.Since(start)

			res := &backendResponse{
				backend: b,
				status:  status,
				body:    body,
				err:     err,
			}

			// Log with a level based on the backend response.
			lvl := level.Debug
			if !res.succeeded() {
				lvl = level.Warn
			}

			lvl(p.logger).Log("msg", "Backend response", "path", r.URL.Path, "query", query, "backend", b.name, "status", status, "elapsed", elapsed, "body", string(body))
			p.metrics.requestDuration.WithLabelValues(res.backend.name, r.Method, p.routeName, strconv.Itoa(res.statusCode())).Observe(elapsed.Seconds())

			// Keep track of the response if required.
			if p.comparator != nil {
				responsesMtx.Lock()
				responses = append(responses, res)
				responsesMtx.Unlock()
			}

			resCh <- res
		}()
	}

	// Wait until all backend requests completed.
	wg.Wait()
	close(resCh)

	// Compare responses.
	if p.comparator != nil {
		expectedResponse := responses[0]
		actualResponse := responses[1]
		if responses[1].backend.preferred {
			expectedResponse, actualResponse = actualResponse, expectedResponse
		}

		result := comparisonSuccess
		err := p.compareResponses(expectedResponse, actualResponse)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "response comparison failed", "route-name", p.routeName,
				"query", r.URL.RawQuery, "err", err)
			result = comparisonFailed
		}

		p.metrics.responsesComparedTotal.WithLabelValues(p.routeName, result).Inc()
	}
}

func (p *ProxyEndpoint) waitBackendResponseForDownstream(resCh chan *backendResponse) *backendResponse {
	var (
		responses                 = make([]*backendResponse, 0, len(p.backends))
		preferredResponseReceived = false
	)

	for res := range resCh {
		// If the response is successful we can immediately return it if:
		// - There's no preferred backend configured
		// - Or this response is from the preferred backend
		// - Or the preferred backend response has already been received and wasn't successful
		if res.succeeded() && (!p.hasPreferredBackend || res.backend.preferred || preferredResponseReceived) {
			return res
		}

		// If we received a non successful response from the preferred backend, then we can
		// return the first successful response received so far (if any).
		if res.backend.preferred && !res.succeeded() {
			preferredResponseReceived = true

			for _, prevRes := range responses {
				if prevRes.succeeded() {
					return prevRes
				}
			}
		}

		// Otherwise we keep track of it for later.
		responses = append(responses, res)
	}

	// No successful response, so let's pick the first one.
	return responses[0]
}

func (p *ProxyEndpoint) compareResponses(expectedResponse, actualResponse *backendResponse) error {
	// compare response body only if we get a 200
	if expectedResponse.status != 200 {
		return fmt.Errorf("skipped comparison of response because we got status code %d from preferred backend's response", expectedResponse.status)
	}

	if actualResponse.status != 200 {
		return fmt.Errorf("skipped comparison of response because we got status code %d from secondary backend's response", actualResponse.status)
	}

	if expectedResponse.status != actualResponse.status {
		return fmt.Errorf("expected status code %d but got %d", expectedResponse.status, actualResponse.status)
	}

	return p.comparator.Compare(expectedResponse.body, actualResponse.body)
}

type backendResponse struct {
	backend *ProxyBackend
	status  int
	body    []byte
	err     error
}

func (r *backendResponse) succeeded() bool {
	if r.err != nil {
		return false
	}

	// We consider the response successful if it's a 2xx or 4xx (but not 429).
	return (r.status >= 200 && r.status < 300) || (r.status >= 400 && r.status < 500 && r.status != 429)
}

func (r *backendResponse) statusCode() int {
	if r.err != nil || r.status <= 0 {
		return 500
	}

	return r.status
}
