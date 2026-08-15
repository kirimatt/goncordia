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
	var jobs []driver.JobRow
	if err := json.NewDecoder(response.Body).Decode(&jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Kind != "admin_test" {
		t.Fatalf("unexpected jobs: %+v", jobs)
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
