package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/admin"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/driver/memory"
)

type args struct{ Value string }

func (args) Kind() string { return "admin_test" }

func TestHandler(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(context.Background(), args{Value: "one"}, nil); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(admin.New(d))
	defer server.Close()

	for _, path := range []string{"/", "/healthz", "/readyz", "/api/queues", "/api/jobs", "/metrics"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status=%d body=%s", path, response.StatusCode, body)
		}
	}

	response, err := http.Get(server.URL + "/api/jobs?queue=default")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var page driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Kind != "admin_test" || page.HasMore || page.NextCursor != "" {
		t.Fatalf("unexpected page: %+v", page)
	}

	response, err = http.Get(server.URL + "/api/jobs?cursor=invalid")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid cursor status=%d, want 400", response.StatusCode)
	}

	response, err = http.Get(server.URL + "/api/jobs?id=missing")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing job status=%d, want 404", response.StatusCode)
	}

	for _, value := range []string{"two", "three"} {
		if _, err := client.Enqueue(context.Background(), args{Value: value}, nil); err != nil {
			t.Fatal(err)
		}
	}
	response, err = http.Get(server.URL + "/api/jobs?limit=2")
	if err != nil {
		t.Fatal(err)
	}
	var firstPage driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&firstPage); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(firstPage.Items) != 2 || !firstPage.HasMore || firstPage.NextCursor == "" {
		t.Fatalf("unexpected first page: %+v", firstPage)
	}
	response, err = http.Get(server.URL + "/api/jobs?limit=2&cursor=" + firstPage.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	var secondPage driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&secondPage); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(secondPage.Items) != 1 || secondPage.HasMore || secondPage.NextCursor != "" {
		t.Fatalf("unexpected second page: %+v", secondPage)
	}

	response, err = http.Post(server.URL+"/api/queues/default/pause", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	queue, err := d.Executor().QueueGet(context.Background(), "default")
	if err != nil || !queue.Paused {
		t.Fatalf("queue not paused: %+v, %v", queue, err)
	}

	metrics, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(metrics.Body)
	metrics.Body.Close()
	if !strings.Contains(string(data), "goncordia_queue_jobs") {
		t.Fatalf("unexpected metrics: %s", data)
	}
}
