package admin_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/admin"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

func TestJobActions(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	server := httptest.NewServer(admin.New(d, admin.WithClock(clk)))
	defer server.Close()

	enqueue := func() string {
		t.Helper()
		result, err := client.Enqueue(ctx, args{Value: "action"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return result.Job.ID
	}
	post := func(path, confirm, body string) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if confirm != "" {
			request.Header.Set("X-Goncordia-Confirm", confirm)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	state := func(id string) driver.JobState {
		t.Helper()
		row, err := d.Executor().JobGetByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return row.State
	}

	cancelled := enqueue()
	if code := post("/api/jobs/"+cancelled+"/cancel", "", ""); code != http.StatusPreconditionRequired {
		t.Fatalf("unconfirmed cancel status=%d, want 428", code)
	}
	if code := post("/api/jobs/"+cancelled+"/cancel", "cancel", ""); code != http.StatusOK || state(cancelled) != driver.JobStateCancelled {
		t.Fatalf("cancel status=%d state=%s", code, state(cancelled))
	}
	if code := post("/api/jobs/"+cancelled+"/cancel", "cancel", ""); code != http.StatusConflict {
		t.Fatalf("second cancel status=%d, want 409", code)
	}

	if code := post("/api/jobs/"+cancelled+"/retry", "retry", ""); code != http.StatusOK || state(cancelled) != driver.JobStateScheduled {
		t.Fatalf("retry status=%d state=%s", code, state(cancelled))
	}

	later := clk.Now().Add(time.Hour).Format(time.RFC3339)
	if code := post("/api/jobs/"+cancelled+"/reschedule", "reschedule", `{"run_at":"`+later+`"}`); code != http.StatusOK {
		t.Fatalf("reschedule status=%d", code)
	}
	row, err := d.Executor().JobGetByID(ctx, cancelled)
	if err != nil || !row.RunAt.Equal(clk.Now().Add(time.Hour)) {
		t.Fatalf("rescheduled row = %+v, %v", row, err)
	}
	if code := post("/api/jobs/"+cancelled+"/reschedule", "reschedule", `{}`); code != http.StatusBadRequest {
		t.Fatalf("reschedule without run_at status=%d, want 400", code)
	}

	if code := post("/api/jobs/"+cancelled+"/delete", "delete", ""); code != http.StatusOK {
		t.Fatalf("delete status=%d", code)
	}
	if _, err := d.Executor().JobGetByID(ctx, cancelled); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("deleted job still readable: %v", err)
	}
	if code := post("/api/jobs/"+cancelled+"/delete", "delete", ""); code != http.StatusNotFound {
		t.Fatalf("delete missing status=%d, want 404", code)
	}

	for path, want := range map[string]int{
		"/api/jobs/x/explode": http.StatusBadRequest,
		"/api/jobs/x":         http.StatusBadRequest,
		"/api/jobs//cancel":   http.StatusBadRequest,
	} {
		if code := post(path, "explode", ""); code != want {
			t.Fatalf("POST %s status=%d, want %d", path, code, want)
		}
	}

	readOnly := httptest.NewServer(admin.New(d, admin.WithReadOnly(true)))
	defer readOnly.Close()
	request, _ := http.NewRequest(http.MethodPost, readOnly.URL+"/api/jobs/"+enqueue()+"/cancel", nil)
	request.Header.Set("X-Goncordia-Confirm", "cancel")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("read-only cancel status=%d, want 403", response.StatusCode)
	}
}

func TestProbe(t *testing.T) {
	if err := admin.Probe(context.Background(), memory.New().Executor()); err != nil {
		t.Fatalf("Probe on memory: %v", err)
	}
	broken := &failingExecutor{Executor: memory.New().Executor(), queueListErr: errors.New("down")}
	if err := admin.Probe(context.Background(), broken); err == nil {
		t.Fatal("Probe ignored a failing store")
	}
}
