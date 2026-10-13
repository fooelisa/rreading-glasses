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

// quotaMinReset is the shortest bucket reset QuotaTransport acts on. Hardcover's
// per-minute bucket resets within 60 s and is already respected by
// HardcoverBatchInterval; blocking on it would only reject batches Hardcover
// would have accepted once the bucket refilled. Only the daily bucket, which
// resets at midnight UTC, is worth stopping for.
const quotaMinReset = time.Minute

// QuotaTransport stops sending requests to Hardcover once its daily quota is
// used up.
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
type QuotaTransport struct {
	http.RoundTripper

	// Reserve is how many requests a single call may spend. A bucket with
	// fewer than Reserve left counts as empty, because a batch of that many
	// fields would be rejected as a whole. Zero means 1.
	Reserve int

	now func() time.Time // now is replaceable for tests.

	mu           sync.Mutex
	blockedUntil time.Time
}

// RoundTrip implements http.RoundTripper.
func (t *QuotaTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	start := t.clock()
	if wait := t.wait(start); wait > 0 {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return quotaExhaustedResponse(r, wait), nil
	}

	resp, err := t.RoundTripper.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	// Hardcover computes t when it answers, so time the block from when the
	// request went out rather than from when the response came back.
	t.observe(r.Context(), start, resp.Header.Get("RateLimit"))
	return resp, nil
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
		remaining, reset := -1, -1
		for param := range strings.SplitSeq(entry, ";") {
			key, val, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok {
				continue
			}
			n, err := strconv.Atoi(val)
			if err != nil {
				continue
			}
			switch key {
			case "r":
				remaining = n
			case "t":
				reset = n
			}
		}
		resetIn := time.Duration(reset) * time.Second
		if remaining >= 0 && remaining < need && resetIn > quotaMinReset {
			wait = max(wait, resetIn)
		}
	}
	return wait
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
