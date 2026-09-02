package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// newSearchTestHandler wires a Handler over a mock getter and an in-memory
// cache, mirroring the setup in controller_test.go.
func newSearchTestHandler(t *testing.T) (*Handler, *Mockgetter) {
	t.Helper()
	getter := NewMockgetter(gomock.NewController(t))
	ctrl, err := NewController(newMemoryCache(), getter, nil, nil)
	require.NoError(t, err)
	go ctrl.Run(t.Context())
	t.Cleanup(func() { ctrl.Shutdown(t.Context()) })
	return NewHandler(ctrl), getter
}

func doSearch(t *testing.T, h *Handler, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.search(rr, httptest.NewRequest(http.MethodGet, "/search?q="+rawQuery, nil))
	return rr
}

// A repeated identical search must hit the upstream getter exactly once.
// /search is uncached upstream, and every miss is a live Hardcover GraphQL
// call against a daily quota, so this is the property that matters.
func TestSearchIsCached(t *testing.T) {
	h, getter := newSearchTestHandler(t)

	getter.EXPECT().
		Search(gomock.Any(), "a repeated query").
		Times(1).
		Return([]SearchResource{{BookID: 1, WorkID: 2}}, nil)

	first := doSearch(t, h, "a+repeated+query")
	require.Equal(t, http.StatusOK, first.Code)

	second := doSearch(t, h, "a+repeated+query")
	require.Equal(t, http.StatusOK, second.Code)

	assert.JSONEq(t, first.Body.String(), second.Body.String(),
		"cached response should be byte-identical to the original")
}

// '+' and %20 both appear in live traffic (one client uses one form, another
// the other). Keying on the raw URL would treat them as different queries and
// halve the hit rate, so the key must be canonical.
func TestSearchCacheKeyIsEncodingInsensitive(t *testing.T) {
	h, getter := newSearchTestHandler(t)

	getter.EXPECT().
		Search(gomock.Any(), "two word query").
		Times(1).
		Return([]SearchResource{{BookID: 3, WorkID: 4}}, nil)

	require.Equal(t, http.StatusOK, doSearch(t, h, "two+word+query").Code)
	require.Equal(t, http.StatusOK, doSearch(t, h, "two%20word%20query").Code)
}

// An empty result is exactly the ambiguous signal produced while Hardcover is
// throttling, so caching it would pin a transient outage in place for the
// whole TTL. Empty responses must always re-query.
func TestSearchDoesNotCacheEmptyResults(t *testing.T) {
	h, getter := newSearchTestHandler(t)

	getter.EXPECT().
		Search(gomock.Any(), "nothing here").
		Times(2).
		Return([]SearchResource{}, nil)

	require.Equal(t, http.StatusOK, doSearch(t, h, "nothing+here").Code)
	require.Equal(t, http.StatusOK, doSearch(t, h, "nothing+here").Code)
}

// Cache-Control must actually reach the client. cacheFor() used to be called
// after WriteHeader, which silently drops headers.
//
// This runs against a real httptest.Server on purpose. httptest.ResponseRecorder
// keeps its header map mutable after WriteHeader, so a recorder-based test
// records headers set too late and passes against the broken ordering -- it
// cannot fail. Only a real ResponseWriter snapshots headers at WriteHeader.
func TestSearchSetsCacheHeaders(t *testing.T) {
	h, getter := newSearchTestHandler(t)
	getter.EXPECT().Search(gomock.Any(), gomock.Any()).AnyTimes().
		Return([]SearchResource{{BookID: 5, WorkID: 6}}, nil)

	srv := httptest.NewServer(http.HandlerFunc(h.search))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/search?q=anything")
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("Cache-Control"), "Cache-Control must reach the client")
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

// DELETE must expire the same key GET writes, or manual busting silently
// does nothing.
func TestSearchDeleteBustsTheCachedEntry(t *testing.T) {
	h, getter := newSearchTestHandler(t)

	getter.EXPECT().
		Search(gomock.Any(), "bust me").
		Times(2).
		Return([]SearchResource{{BookID: 7, WorkID: 8}}, nil)

	require.Equal(t, http.StatusOK, doSearch(t, h, "bust+me").Code)

	del := httptest.NewRecorder()
	h.search(del, httptest.NewRequest(http.MethodDelete, "/search?q=bust+me", nil))
	require.Equal(t, http.StatusOK, del.Code)

	require.Equal(t, http.StatusOK, doSearch(t, h, "bust+me").Code)
}
