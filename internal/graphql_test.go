package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blampe/rreading-glasses/gr"
	"github.com/blampe/rreading-glasses/hardcover"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestQueryBuilderMultipleQueries(t *testing.T) {
	t.Run("hardcover", func(t *testing.T) {
		qb := newQueryBuilder()

		query1 := hardcover.GetWork_Operation
		vars1 := map[string]any{"grBookIDs": []string{"1"}}

		query2 := hardcover.GetAuthorEditions_Operation
		vars2 := map[string]any{
			"id":     1,
			"limit":  2,
			"offset": 3,
		}

		id1, _, err := qb.add(query1, vars1)
		require.NoError(t, err)

		id2, _, err := qb.add(query2, vars2)
		require.NoError(t, err)

		query, vars, err := qb.build()
		require.NoError(t, err)

		expected := fmt.Sprintf(`query GetWork($%s_bookID: Int!, $%s_id: Int!, $%s_limit: Int!, $%s_offset: Int!) {
  %s: books_by_pk(id: $%s_bookID) {
    ...WorkInfo
    editions(order_by: {score: desc_nulls_last}) {
      ...EditionInfo
    }
  }
  %s: authors_by_pk(id: $%s_id) {
    ...AuthorInfo
    contributions(limit: $%s_limit, offset: $%s_offset, order_by: {book: {ratings_count: desc}}, where: {contributable_type: {_eq: "Book"}, book: {book_status_id: {_eq: "1"}}}) {
      ...Contributions
      book {
        id
        title
        ratings_count
        ...DefaultEditions
      }
    }
  }
}
fragment AuthorInfo on authors {
  id
  name
  slug
  bio
  cached_image(path: "url")
}
fragment Contributions on contributions {
  contribution
  author {
    ...AuthorInfo
  }
}
fragment DefaultEditions on books {
  id
  contributions {
    ...Contributions
  }
  default_audio_edition {
    id
    contributions {
      ...Contributions
    }
  }
  default_physical_edition {
    id
    contributions {
      ...Contributions
    }
  }
  default_cover_edition {
    id
    contributions {
      ...Contributions
    }
  }
  default_ebook_edition {
    id
    contributions {
      ...Contributions
    }
  }
  fallback: editions(order_by: {id: desc}, limit: 1) {
    id
  }
}
fragment EditionInfo on editions {
  id
  title
  subtitle
  asin
  isbn_13
  edition_format
  pages
  audio_seconds
  language {
    code3
  }
  publisher {
    name
  }
  release_date
  audio_seconds
  physical_format
  physical_information
  edition_information
  users_read_count
  book_id
  score
}
fragment WorkInfo on books {
  id
  title
  subtitle
  description
  release_date
  cached_tags(path: "$.Genre")
  cached_image(path: "url")
  slug
  state
  canonical_id
  book_series {
    position
    series {
      id
      name
      description
    }
  }
  rating
  ratings_count
  ...DefaultEditions
}`, id1, id2, id2, id2, id1, id1, id2, id2, id2, id2)

		assert.Equal(t, expected, query, query)

		assert.Len(t, vars, 4)
		assert.Contains(t, vars, id1+"_bookID", id2+"_id", id2+"_limit", id2+"_offset")
	})

	t.Run("gr", func(t *testing.T) {
		qb := newQueryBuilder()

		query1 := gr.GetBook_Operation
		vars1 := map[string]any{"legacyId": []string{"1"}}

		query2 := gr.GetAuthorWorks_Operation
		vars2 := map[string]any{
			"pagination":                 map[string]string{},
			"getWorksByContributorInput": map[string]string{},
		}

		id1, _, err := qb.add(query1, vars1)
		require.NoError(t, err)

		id2, _, err := qb.add(query2, vars2)
		require.NoError(t, err)

		query, vars, err := qb.build()
		require.NoError(t, err)

		expected := fmt.Sprintf(`query GetBook($%s_legacyId: Int!, $%s_getWorksByContributorInput: GetWorksByContributorInput!, $%s_pagination: PaginationInput!) {
  %s: getBookByLegacyId(legacyId: $%s_legacyId) {
    ...BookInfo
    work {
      id
      legacyId
      details {
        webUrl
        publicationTime
      }
      bestBook {
        legacyId
        title
        titlePrimary
        primaryContributorEdge {
          role
          node {
            legacyId
          }
        }
      }
      editions {
        edges {
          node {
            ...BookInfo
          }
        }
      }
    }
  }
  %s: getWorksByContributor(getWorksByContributorInput: $%s_getWorksByContributorInput, pagination: $%s_pagination) {
    edges {
      node {
        id
        bestBook {
          legacyId
          primaryContributorEdge {
            role
            node {
              legacyId
            }
          }
          secondaryContributorEdges {
            role
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      nextPageToken
    }
  }
}
fragment BookInfo on Book {
  id
  legacyId
  description(stripped: true)
  bookGenres {
    genre {
      name
    }
  }
  bookSeries {
    series {
      id
      title
      webUrl
    }
    seriesPlacement
  }
  details {
    asin
    isbn13
    format
    numPages
    language {
      name
    }
    officialUrl
    publisher
    publicationTime
  }
  imageUrl
  primaryContributorEdge {
    node {
      id
      name
      legacyId
      webUrl
      profileImageUrl
      description
    }
  }
  stats {
    averageRating
    ratingsCount
    ratingsSum
  }
  title
  titlePrimary
  webUrl
}`, id1, id2, id2, id1, id1, id2, id2, id2)

		assert.Equal(t, expected, query)

		assert.Len(t, vars, 3)
		assert.Contains(t, vars, id1+"_legacyId", id2+"_getWorksByContributorInput", id2+"_pagination")
	})
}

func TestBatching(t *testing.T) {
	apiKey := os.Getenv("HARDCOVER_API_KEY")
	if apiKey == "" {
		t.Skip("missing HARDCOVER_API_KEY")
		return
	}
	transport := &HeaderTransport{
		Key:          "Authorization",
		Value:        "Bearer " + apiKey,
		RoundTripper: http.DefaultTransport,
	}

	client := &http.Client{Transport: transport}

	url := "https://api.hardcover.app/v1/graphql"

	gql, err := NewBatchedGraphQLClient(url, client, time.Second, 6, nil)
	require.NoError(t, err)

	start := time.Now()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		_, err := hardcover.GetWork(context.Background(), gql, 156028352)
		if err != nil {
			panic(err)
		}
	})

	wg.Go(func() {
		_, err := hardcover.GetWork(context.Background(), gql, 164005178)
		if err != nil {
			panic(err)
		}
	})

	wg.Go(func() {
		_, err := hardcover.GetWork(context.Background(), gql, 340640138)
		if err != nil {
			panic(err)
		}
	})

	wg.Go(func() {
		_, err := hardcover.GetWork(context.Background(), gql, -1) // Missing.
		if err != nil {
			panic(err)
		}
	})

	wg.Wait()

	assert.Less(t, time.Since(start), 4*time.Second)
}

func TestBatchingOverflow(t *testing.T) {
	calls := atomic.Int32{}

	client := &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			body := `{"data": {}, "errors": []}`
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	}

	gql, err := NewBatchedGraphQLClient("https://foo.com", client, 50*time.Millisecond, 1, nil)
	require.NoError(t, err)

	wg := sync.WaitGroup{}

	// var resp1, resp2 *gr.GetBookResponse
	var err1, err2 error

	// Spawn more queries than our batch allows. They should get executed in
	// separate batches.
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err1 = gr.GetBook(t.Context(), gql, 1)
	}()
	go func() {
		defer wg.Done()
		_, err2 = gr.GetBook(t.Context(), gql, 2)
	}()
	wg.Wait()

	assert.NoError(t, err1)
	assert.NoError(t, err2)

	assert.Equal(t, int32(2), calls.Load())
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func TestGQLStatusCode(t *testing.T) {
	err := &gqlerror.Error{Message: "womp"}
	assert.ErrorIs(t, err, gqlStatusErr(err))

	err = &gqlerror.Error{Message: "Request failed with status code 403"}
	err403 := statusErr(403)
	assert.ErrorAs(t, gqlStatusErr(err), &err403)
}

// TestHardcoverBatchNeverExceedsBurstCapacity asserts the invariant Hardcover
// actually enforces: no request we send may contain more than
// HardcoverMaxBatchSize TOP-LEVEL FIELDS.
//
// Hardcover counts top-level fields per request, separately from the 60/min
// rate limit, and answers HTTP 403 "request_exceeds_capacity" when a request
// exceeds the tier's burst capacity. The whole batch fails, so every query
// riding in it fails. Observed in production 2026-09-17 at 64 rejections/hour
// with batches of 15, 9 and 6, which surfaced as /search returning HTTP 500.
//
// This parses the real outgoing request rather than inspecting internal state,
// so it also guards against a future change to how batches are assembled - not
// just against someone raising the constant back to upstream's 25.
func TestHardcoverBatchNeverExceedsBurstCapacity(t *testing.T) {
	// The limit as Hardcover states it, written out independently of
	// HardcoverMaxBatchSize ON PURPOSE. Asserting against the constant the
	// client was built from is self-referential and passes for ANY value -
	// including upstream's 25, which is the bug this test exists to catch.
	const hardcoverFreeTierBurstCapacity = 5

	// Tripwire: raising the constant past what the API accepts must fail here
	// rather than in production as a 403 storm.
	require.LessOrEqual(t, HardcoverMaxBatchSize, hardcoverFreeTierBurstCapacity,
		"HardcoverMaxBatchSize exceeds the free tier's documented burst capacity")

	var mu sync.Mutex
	var topLevelCounts []int

	client := &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			var body struct {
				Query string `json:"query"`
			}
			raw, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(raw, &body))

			doc, err := parser.Parse(parser.ParseParams{
				Source: source.NewSource(&source.Source{Body: []byte(body.Query)}),
			})
			require.NoError(t, err, "outgoing query must be valid GraphQL")

			for _, def := range doc.Definitions {
				op, ok := def.(*ast.OperationDefinition)
				if !ok || op.GetSelectionSet() == nil {
					continue
				}
				mu.Lock()
				topLevelCounts = append(topLevelCounts, len(op.GetSelectionSet().Selections))
				mu.Unlock()
			}

			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{"data": {}, "errors": []}`)),
			}, nil
		}),
	}

	gql, err := NewBatchedGraphQLClient("https://foo.com", client, 20*time.Millisecond, HardcoverMaxBatchSize, nil)
	require.NoError(t, err)

	// Comfortably more concurrent queries than one batch may carry.
	const queries = 4 * hardcoverFreeTierBurstCapacity

	wg := sync.WaitGroup{}
	wg.Add(queries)
	for i := range queries {
		go func(i int) {
			defer wg.Done()
			_, _ = gr.GetBook(t.Context(), gql, int64(i))
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.NotEmpty(t, topLevelCounts, "expected at least one outgoing request")
	for _, n := range topLevelCounts {
		assert.LessOrEqual(t, n, hardcoverFreeTierBurstCapacity,
			"a request carried %d top-level fields; Hardcover rejects anything over %d with request_exceeds_capacity",
			n, hardcoverFreeTierBurstCapacity)
	}
}
