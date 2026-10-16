package internal

import (
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// quotaMinReset separates the two kinds of bucket. One that resets in more
// than this (the daily bucket, at midnight UTC) is worth stopping for: requests
// are answered locally with a 429 until it resets. One that resets sooner (the
// per-minute bucket) is worth waiting for: the next request is delayed until
// the bucket holds enough for it, then sent.
const quotaMinReset = time.Minute

// paceMargin is added to every per-minute wait. Hardcover measures the bucket
// when it receives a request, not when we send it, so network jitter can make
// a request that leaves exactly on time arrive one refill too early.
const paceMargin = 500 * time.Millisecond

// defaultRefill is how long one request takes to come back into a bucket when
// the RateLimit-Policy header does not say. Hardcover's free tier is
// q=60;w=60, i.e. one per second.
const defaultRefill = time.Second

// QuotaTransport stops sending requests to Hardcover once its daily quota is
// used up, and delays requests until the per-minute bucket can take them.
//
// Hardcover answers every request with a RateLimit header (IETF draft format):
//
//	RateLimit: "Free";r=8;t=0, "daily";r=4231;t=51234
//
// r is the requests left in a bucket and t the seconds until it refills. Every
// top-level GraphQL field counts as one request, and the free tier allows 5,000
// a day. Once the daily bucket is empty, every request returns 429 until
// midnight UTC, so sending more only produces more 429s. While blocked, this
// transport answers locally with a 429, the same status a real rejection
// gets, and no request reaches Hardcover.
//
// The per-minute bucket holds 5 (burst) and refills one request per second
// (RateLimit-Policy: "Free (JWT)";q=60;w=60;burst=5). A full 5-field batch
// empties it, and HardcoverBatchInterval sends the next one exactly 5 s later,
// so any jitter or extra call meant a 429 (~380 a day, 2026-10-14..15). After
// each response the transport works out when the bucket will hold Reserve
// again, and holds the next request until then.
type QuotaTransport struct {
	http.RoundTripper

	// Reserve is how many requests a single call may spend. A bucket with
	// fewer than Reserve left counts as empty, because a batch of that many
	// fields would be rejected as a whole. Zero means 1.
	Reserve int

	now func() time.Time // now is replaceable for tests.

	sleep func(context.Context, time.Duration) error // sleep is replaceable for tests.

	mu           sync.Mutex
	blockedUntil time.Time
	notBefore    time.Time
	turn         chan struct{} // turn lets one request at a time pace and send.
}

// RoundTrip implements http.RoundTripper.
//
// Requests take turns: each one waits for the per-minute bucket and is sent
// only after the previous one has been answered, so its wait is based on the
// latest RateLimit header. Without this, requests that waited together would
// all be sent at the same moment and all but the first would get a 429. The
// batch loop sends one request at a time anyway, so this costs it nothing.
func (t *QuotaTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := t.takeTurn(r.Context()); err != nil {
		closeBody(r)
		return nil, err
	}
	defer t.endTurn()

	var start time.Time
	for {
		start = t.clock()
		// Check the daily block again after every wait: another request's
		// answer may have set it while this one waited.
		if wait := t.wait(start); wait > 0 {
			closeBody(r)
			return quotaExhaustedResponse(r, wait), nil
		}
		pace := t.pace(start)
		if pace <= 0 {
			break
		}
		Log(r.Context()).Debug("waiting for hardcover per-minute bucket", "wait", pace.String())
		if err := t.doSleep(r.Context(), pace); err != nil {
			closeBody(r)
			return nil, err
		}
	}

	resp, err := t.RoundTripper.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	// Hardcover computes t when it answers, so time the block from when the
	// request went out rather than from when the response came back.
	t.observe(r.Context(), start, resp.Header.Get("RateLimit"))
	// The per-minute bucket refills continuously, so time its wait from when
	// the answer arrived: that is the latest moment its r can describe.
	t.observePace(t.clock(), resp.Header.Get("RateLimit"), resp.Header.Get("RateLimit-Policy"))
	return resp, nil
}

// takeTurn blocks until no other request is pacing or in flight.
func (t *QuotaTransport) takeTurn(ctx context.Context) error {
	t.mu.Lock()
	if t.turn == nil {
		t.turn = make(chan struct{}, 1)
	}
	turn := t.turn
	t.mu.Unlock()

	select {
	case turn <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *QuotaTransport) endTurn() {
	t.mu.Lock()
	turn := t.turn
	t.mu.Unlock()
	<-turn
}

func closeBody(r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}
}

// pace returns how long the next request must wait for the per-minute
// bucket at now, or 0.
func (t *QuotaTransport) pace(now time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.notBefore.Sub(now)
}

// observePace holds the next request until the per-minute bucket has
// refilled enough for it.
func (t *QuotaTransport) observePace(now time.Time, header, policy string) {
	wait := parseRateLimitPace(header, policy, max(t.Reserve, 1))
	if wait <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if until := now.Add(wait + paceMargin); until.After(t.notBefore) {
		t.notBefore = until
	}
}

func (t *QuotaTransport) doSleep(ctx context.Context, d time.Duration) error {
	if t.sleep != nil {
		return t.sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// wait returns how long requests are still blocked at now, or 0.
func (t *QuotaTransport) wait(now time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.blockedUntil.Sub(now)
}

// observe blocks requests until the latest reset of any exhausted bucket.
func (t *QuotaTransport) observe(ctx context.Context, start time.Time, header string) {
	wait := parseRateLimitWait(header, max(t.Reserve, 1))
	if wait <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	until := start.Add(wait)
	if !until.After(t.blockedUntil) {
		return
	}
	t.blockedUntil = until
	Log(ctx).Warn("hardcover quota exhausted, pausing requests", "until", until.UTC().Format(time.RFC3339), "rateLimit", header)
}

func (t *QuotaTransport) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// parseRateLimitWait reads a RateLimit header and returns how long to wait
// before the next request: the longest reset of any bucket that has fewer
// than need requests left and resets in more than quotaMinReset, or 0.
// Entries it cannot parse are ignored.
func parseRateLimitWait(header string, need int) time.Duration {
	var wait time.Duration
	for entry := range strings.SplitSeq(header, ",") {
		_, params := splitEntry(entry)
		remaining, hasR := params["r"]
		reset, hasT := params["t"]
		resetIn := time.Duration(reset) * time.Second
		if hasR && hasT && remaining < need && resetIn > quotaMinReset {
			wait = max(wait, resetIn)
		}
	}
	return wait
}

// parseRateLimitPace reads a RateLimit header and returns how long until
// every short-reset bucket (reset within quotaMinReset) holds need requests
// again, or 0. Each bucket's refill time per request comes from the matching
// RateLimit-Policy entry (w/q), or defaultRefill if there is none. A short
// bucket's t only caps the wait: Hardcover reports t=0 for the per-minute
// bucket while it refills. Entries without both r and t are ignored.
func parseRateLimitPace(header, policy string, need int) time.Duration {
	refill := parseRefill(policy)
	var wait time.Duration
	for entry := range strings.SplitSeq(header, ",") {
		name, params := splitEntry(entry)
		remaining, hasR := params["r"]
		reset, hasT := params["t"]
		resetIn := time.Duration(reset) * time.Second
		// Skip full buckets, buckets of unknown kind (no t), and long buckets,
		// which parseRateLimitWait handles.
		if !hasR || !hasT || remaining >= need || resetIn > quotaMinReset {
			continue
		}
		per, ok := refill[name]
		if !ok {
			per = defaultRefill
		}
		refilled := time.Duration(need-remaining) * per
		if resetIn > 0 {
			refilled = min(refilled, resetIn) // the bucket is full again at its reset.
		}
		wait = max(wait, refilled)
	}
	return wait
}

// parseRefill maps each RateLimit-Policy entry name to how long one request
// takes to refill (w/q). Entries without a usable q and w are left out.
func parseRefill(policy string) map[string]time.Duration {
	refill := map[string]time.Duration{}
	for entry := range strings.SplitSeq(policy, ",") {
		name, params := splitEntry(entry)
		q, w := params["q"], params["w"]
		if name == "" || q <= 0 || w <= 0 {
			continue
		}
		refill[name] = time.Duration(w) * time.Second / time.Duration(q)
	}
	return refill
}

// splitEntry splits one structured-field entry such as `"daily";r=4;t=9` into
// its name and its integer parameters. Parameters that are not integers are
// dropped.
func splitEntry(entry string) (string, map[string]int) {
	parts := strings.Split(entry, ";")
	params := map[string]int{}
	for _, param := range parts[1:] {
		key, val, ok := strings.Cut(strings.TrimSpace(param), "=")
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(val); err == nil {
			params[key] = n
		}
	}
	return strings.TrimSpace(parts[0]), params
}

// quotaExhaustedResponse is the local stand-in for Hardcover's own 429.
func quotaExhaustedResponse(r *http.Request, wait time.Duration) *http.Response {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	body := `{"error":"Too Many Requests","message":"Hardcover quota exhausted; not sent"}`
	return &http.Response{
		Status:        "429 Too Many Requests",
		StatusCode:    http.StatusTooManyRequests,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       r,
	}
}
