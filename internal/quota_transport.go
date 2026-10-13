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

// QuotaTransport stops sending requests to Hardcover while one of its rate
// limit buckets is empty.
//
// Hardcover answers every request with a RateLimit header (IETF draft format):
//
//	RateLimit: "Free";r=8;t=0, "daily";r=4231;t=51234
//
// r is the requests left in a bucket and t the seconds until it refills. Every
// top-level GraphQL field counts as one request, and the free tier allows 5,000
// a day. Once the daily bucket is empty, every request returns 429 until
// midnight UTC, so sending more only produces more 429s. While a bucket is
// empty this transport answers locally with a 429, which callers already
// handle, and no request reaches Hardcover.
type QuotaTransport struct {
	http.RoundTripper

	now func() time.Time // now is replaceable for tests.

	mu           sync.Mutex
	blockedUntil time.Time
}

// RoundTrip implements http.RoundTripper.
func (t *QuotaTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if wait := t.wait(); wait > 0 {
		return quotaExhaustedResponse(r, wait), nil
	}

	resp, err := t.RoundTripper.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	t.observe(r.Context(), resp.Header.Get("RateLimit"))
	return resp, nil
}

// wait returns how long requests are still blocked, or 0.
func (t *QuotaTransport) wait() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.blockedUntil.Sub(t.clock())
}

// observe blocks requests until the latest reset of any empty bucket.
func (t *QuotaTransport) observe(ctx context.Context, header string) {
	wait := parseRateLimitWait(header)
	if wait <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	until := t.clock().Add(wait)
	if !until.After(t.blockedUntil) {
		return
	}
	t.blockedUntil = until
	// The per-minute bucket empties for a second or two at a time; only an
	// empty daily bucket is worth a log line.
	if wait > time.Minute {
		Log(ctx).Warn("hardcover quota exhausted, pausing requests", "until", until.UTC().Format(time.RFC3339), "rateLimit", header)
	}
}

func (t *QuotaTransport) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// parseRateLimitWait reads a RateLimit header and returns how long to wait
// before the next request: the longest reset time of any empty bucket, or 0
// when every bucket has requests left. Entries it cannot parse are ignored.
func parseRateLimitWait(header string) time.Duration {
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
		if remaining == 0 && reset > 0 {
			wait = max(wait, time.Duration(reset)*time.Second)
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
