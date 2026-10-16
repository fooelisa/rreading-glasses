package internal

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// rateLimitResponse returns an upstream response carrying the given header.
func rateLimitResponse(status int, rateLimit string) *http.Response {
	h := http.Header{}
	h.Set("RateLimit", rateLimit)
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader("{}"))}
}

func newHardcoverRequest(t *testing.T) *http.Request {
	req, err := http.NewRequest(http.MethodPost, "https://api.hardcover.app/v1/graphql", strings.NewReader("{}"))
	require.NoError(t, err)
	return req
}

func TestParseRateLimitWait(t *testing.T) {
	tests := []struct {
		name   string
		header string
		need   int
		want   time.Duration
	}{
		{"empty header", "", 1, 0},
		{"tokens left", `"Free";r=8;t=0, "daily";r=4231;t=51234`, 5, 0},
		{"daily exhausted, plan entry dropped", `"daily";r=0;t=11888`, 1, 11888 * time.Second},
		{"daily below one batch", `"Free";r=5;t=0, "daily";r=3;t=40000`, 5, 40000 * time.Second},
		{"daily exactly one batch left", `"daily";r=5;t=40000`, 5, 0},
		{"minute bucket empty is left to pacing", `"Free (JWT)";r=0;t=3, "daily";r=40;t=500`, 5, 0},
		{"both empty takes the daily wait", `"Free";r=0;t=3, "daily";r=0;t=900`, 1, 900 * time.Second},
		{"garbage is ignored", `nonsense;;r=x;t=y`, 1, 0},
		{"empty bucket without t is ignored", `"daily";r=0`, 1, 0},
		{"bucket without r is ignored", `"daily";t=900`, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseRateLimitWait(tt.header, tt.need))
		})
	}
}

func TestQuotaTransportBlocksUntilReset(t *testing.T) {
	// Arrange
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimitResponse(http.StatusOK, `"daily";r=0;t=3600`), nil
	})
	qt := &QuotaTransport{RoundTripper: upstream, now: func() time.Time { return now }}

	// Act: the first call goes out and reports the daily bucket empty.
	resp, err := qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Assert: while blocked, calls are answered locally with a 429.
	now = now.Add(59 * time.Minute)
	resp, err = qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "60", resp.Header.Get("Retry-After"))
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "Too Many Requests")
	assert.Equal(t, 1, calls, "a blocked request must not reach Hardcover")

	// Assert: after the reset, requests go out again.
	now = now.Add(time.Minute)
	_, err = qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestQuotaTransportTimesBlockFromRequestStart(t *testing.T) {
	// Arrange: the response takes 30 s to arrive.
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		now = now.Add(30 * time.Second)
		return rateLimitResponse(http.StatusTooManyRequests, `"daily";r=0;t=120`), nil
	})
	qt := &QuotaTransport{RoundTripper: upstream, now: func() time.Time { return now }}
	start := now

	// Act
	_, err := qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)

	// Assert: blocked until start+120 s, not arrival+120 s.
	assert.Equal(t, start.Add(120*time.Second), qt.blockedUntil)
}

func TestQuotaTransportLateResponseDoesNotShortenBlock(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	qt := &QuotaTransport{now: func() time.Time { return now }}
	qt.observe(t.Context(), now, `"daily";r=0;t=3600`)
	until := qt.blockedUntil

	qt.observe(t.Context(), now, `"daily";r=0;t=120`)

	assert.Equal(t, until, qt.blockedUntil)
}

func TestQuotaTransportUsesReserve(t *testing.T) {
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimitResponse(http.StatusOK, `"daily";r=3;t=40000`), nil
	})
	qt := &QuotaTransport{RoundTripper: upstream, Reserve: 5}

	_, _ = qt.RoundTrip(newHardcoverRequest(t))
	resp, err := qt.RoundTrip(newHardcoverRequest(t))

	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, 1, calls, "3 left is less than one batch of 5")
}

func TestQuotaTransportPassesThroughWithTokensLeft(t *testing.T) {
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimitResponse(http.StatusOK, `"Free";r=5;t=0, "daily";r=100;t=500`), nil
	})
	qt := &QuotaTransport{
		RoundTripper: upstream,
		Reserve:      5,
		sleep: func(context.Context, time.Duration) error {
			t.Fatal("a bucket with a full batch left must not delay the request")
			return nil
		},
	}

	for range 3 {
		resp, err := qt.RoundTrip(newHardcoverRequest(t))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
	assert.Equal(t, 3, calls)
}

func TestQuotaTransportKeepsUpstreamErrors(t *testing.T) {
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	qt := &QuotaTransport{RoundTripper: upstream}

	_, err := qt.RoundTrip(newHardcoverRequest(t))
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestQuotaTransportConcurrent(t *testing.T) {
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return rateLimitResponse(http.StatusOK, `"daily";r=0;t=3600`), nil
	})
	qt := &QuotaTransport{RoundTripper: upstream}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			resp, err := qt.RoundTrip(newHardcoverRequest(t))
			if err == nil {
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()

	assert.Positive(t, qt.wait(time.Now()))
}

func TestParseRateLimitPace(t *testing.T) {
	const policy = `"Free (JWT)";q=60;w=60;burst=5, "daily";q=5000;w=86400`
	tests := []struct {
		name   string
		header string
		policy string
		need   int
		want   time.Duration
	}{
		{"empty header", "", policy, 5, 0},
		{"enough left", `"Free (JWT)";r=5;t=0, "daily";r=4000;t=900`, policy, 5, 0},
		{"one short", `"Free (JWT)";r=4;t=0, "daily";r=4993;t=20523`, policy, 5, time.Second},
		{"bucket empty after a full batch", `"Free (JWT)";r=0;t=0`, policy, 5, 5 * time.Second},
		{"refill from policy", `"Free";r=0;t=0`, `"Free";q=30;w=60`, 1, 2 * time.Second},
		{"no policy uses the default refill", `"Free";r=2;t=0`, "", 5, 3 * time.Second},
		{"daily bucket is left to parseRateLimitWait", `"Free (JWT)";r=5;t=0, "daily";r=0;t=900`, policy, 5, 0},
		{"bucket without r is ignored", `"Free (JWT)";t=0`, policy, 5, 0},
		{"bucket without t is ignored", `"daily";r=0`, policy, 5, 0},
		{"wait is capped at the bucket's reset", `"Free";r=0;t=2`, "", 5, 2 * time.Second},
		{"garbage is ignored", `nonsense;;r=x;t=y`, `junk;q=0;w=x`, 5, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseRateLimitPace(tt.header, tt.policy, tt.need))
		})
	}
}

func TestQuotaTransportWaitsForMinuteBucket(t *testing.T) {
	// Arrange: Hardcover reports the per-minute bucket empty after a batch.
	now := time.Date(2026, 10, 16, 19, 0, 0, 0, time.UTC)
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		resp := rateLimitResponse(http.StatusOK, `"Free (JWT)";r=0;t=0, "daily";r=4000;t=900`)
		resp.Header.Set("RateLimit-Policy", `"Free (JWT)";q=60;w=60;burst=5`)
		return resp, nil
	})
	var slept []time.Duration
	qt := &QuotaTransport{
		RoundTripper: upstream,
		Reserve:      5,
		now:          func() time.Time { return now },
		sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			now = now.Add(d)
			return nil
		},
	}

	// Act: one batch, then the next one 1 s later.
	_, err := qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)
	now = now.Add(time.Second)
	resp, err := qt.RoundTrip(newHardcoverRequest(t))

	// Assert: the second waits out the refill plus margin, then is sent.
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, calls)
	assert.Equal(t, []time.Duration{4*time.Second + paceMargin}, slept)
}

func TestQuotaTransportMinuteWaitHonoursContext(t *testing.T) {
	// Arrange
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimitResponse(http.StatusOK, `"Free (JWT)";r=0;t=0`), nil
	})
	qt := &QuotaTransport{RoundTripper: upstream, Reserve: 5}
	_, err := qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)

	// Act: the caller gives up while the request waits.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = qt.RoundTrip(newHardcoverRequest(t).WithContext(ctx))

	// Assert
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "a cancelled request must not reach Hardcover")
}

func TestQuotaTransportRechecksDailyBlockAfterWait(t *testing.T) {
	// Arrange: the per-minute bucket is empty after the first call.
	now := time.Date(2026, 10, 16, 19, 0, 0, 0, time.UTC)
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimitResponse(http.StatusOK, `"Free (JWT)";r=0;t=0`), nil
	})
	var qt *QuotaTransport
	qt = &QuotaTransport{
		RoundTripper: upstream,
		Reserve:      5,
		now:          func() time.Time { return now },
		sleep: func(ctx context.Context, d time.Duration) error {
			// Another request's answer empties the daily bucket meanwhile.
			qt.observe(ctx, now, `"daily";r=0;t=3600`)
			now = now.Add(d)
			return nil
		},
	}
	_, err := qt.RoundTrip(newHardcoverRequest(t))
	require.NoError(t, err)

	// Act
	resp, err := qt.RoundTrip(newHardcoverRequest(t))

	// Assert
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, 1, calls, "a request blocked during its wait must not reach Hardcover")
}

func TestQuotaTransportConcurrentWaitersAreSpaced(t *testing.T) {
	// Arrange: every answer reports the per-minute bucket empty, so each
	// request must wait a full refill after the previous answer.
	var clockMu sync.Mutex
	now := time.Date(2026, 10, 16, 19, 0, 0, 0, time.UTC)
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	var sent []time.Time
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		sent = append(sent, clock()) // only one request is in flight at a time
		resp := rateLimitResponse(http.StatusOK, `"Free (JWT)";r=0;t=0`)
		resp.Header.Set("RateLimit-Policy", `"Free (JWT)";q=60;w=60;burst=5`)
		return resp, nil
	})
	qt := &QuotaTransport{
		RoundTripper: upstream,
		Reserve:      5,
		now:          clock,
		sleep: func(_ context.Context, d time.Duration) error {
			clockMu.Lock()
			now = now.Add(d)
			clockMu.Unlock()
			return nil
		},
	}

	// Act
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			resp, err := qt.RoundTrip(newHardcoverRequest(t))
			if err == nil {
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()

	// Assert
	require.Len(t, sent, 5)
	for i := 1; i < len(sent); i++ {
		assert.GreaterOrEqual(t, sent[i].Sub(sent[i-1]), 5*time.Second, "request %d sent too soon", i)
	}
}
