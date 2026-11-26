package admin_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/kirimatt/goncordia/v2/admin"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// ExampleNew mounts the dashboard behind authentication at /admin/.
func ExampleNew() {
	// This token is only for illustration; use real authentication in an application.
	requireAuth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer example-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	d := memory.New()
	mux := http.NewServeMux()
	mux.Handle("/admin/", requireAuth(http.StripPrefix("/admin", admin.New(d))))

	request := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	fmt.Println(response.Code)

	request = httptest.NewRequest(http.MethodGet, "/admin/", nil)
	request.Header.Set("Authorization", "Bearer example-token")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	fmt.Println(response.Code)

	// Output:
	// 401
	// 200
}
