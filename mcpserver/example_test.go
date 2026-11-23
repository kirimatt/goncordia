package mcpserver_test

import (
	"context"
	"errors"
	"log"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/kirimatt/goncordia/mcpserver/v2"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// Serve the tools over Streamable HTTP next to the admin dashboard, behind
// the application's own authentication.
func ExampleHandler() {
	d := memory.New() // your driver, e.g. pgxdriver.New(pool)
	srv := mcpserver.New(d, mcpserver.WithName("goncordia-staging"))

	requireAdmin := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+adminToken() {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	http.Handle("/mcp", requireAdmin(mcpserver.Handler(srv)))
}

// A command that a local agent (Claude Code, Cursor) starts over stdio,
// pointed at a development database. WithWriteTools lets the agent retry,
// cancel and delete jobs; leave it out for a read-only server.
func ExampleNew_stdio() {
	d := memory.New() // your driver, e.g. pgxdriver.New(pool)
	srv := mcpserver.New(d, mcpserver.WithWriteTools())
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func adminToken() string { return "secret" }
