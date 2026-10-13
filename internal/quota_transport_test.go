package internal

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestParseRateLimitWait(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"empty header", "", 0},
		{"tokens left", `"Free";r=8;t=0, "daily";r=4231;t=51234`, 0},
		{"daily exhausted, plan entry dropped", `"daily";r=0;t=11888`, 11888 * time.Second},
		{"minute bucket empty", `"Free (JWT)";r=0;t=3, "daily";r=40;t=500`, 3 * time.Second},
		{"both empty takes the longer wait", `"Free";r=0;t=3, "daily";r=0;t=900`, 900 * time.Second},
		{"no spaces", `"daily";r=0;t=60`, 60 * time.Second},
		{"garbage is ignored", `nonsense;;r=x;t=y`, 0},
		{"empty bucket without t is ignored", `"daily";r=0`, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseRateLimitWait(tt.header))
		})
	}
}

func TestQuotaTransportBlocksUntilReset(t *testing.T) {
	// Arrange
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		h := http.Header{}
		h.Set("RateLimit", `"daily";r=0;t=3600`)
		return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	qt := &QuotaTransport{RoundTripper: upstream, now: func() time.Time { return now }}
	req, _ := http.NewRequest(http.MethodPost, "https://api.hardcover.app/v1/graphql", nil)

	// Act: the first call goes out and reports the daily bucket empty.
	resp, err := qt.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Assert: while blocked, calls are answered locally with a 429.
	now = now.Add(59 * time.Minute)
	resp, err = qt.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "60", resp.Header.Get("Retry-After"))
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "Too Many Requests")
	assert.Equal(t, 1, calls, "a blocked request must not reach Hardcover")

	// Assert: after the reset, requests go out again.
	now = now.Add(time.Minute)
	_, err = qt.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestQuotaTransportPassesThroughWithTokensLeft(t *testing.T) {
	calls := 0
	upstream := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		h := http.Header{}
		h.Set("RateLimit", `"Free";r=4;t=0, "daily";r=100;t=500`)
		return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	qt := &QuotaTransport{RoundTripper: upstream}
	req, _ := http.NewRequest(http.MethodPost, "https://api.hardcover.app/v1/graphql", nil)

	for range 3 {
		resp, err := qt.RoundTrip(req)
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
	req, _ := http.NewRequest(http.MethodPost, "https://api.hardcover.app/v1/graphql", nil)

	_, err := qt.RoundTrip(req)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
