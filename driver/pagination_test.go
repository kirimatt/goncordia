package driver_test

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/driver"
)

func TestJobCursorRoundTrip(t *testing.T) {
	row := driver.JobRow{ID: "job/42", CreatedAt: time.Date(2026, 8, 16, 12, 34, 56, 789, time.FixedZone("test", 3600))}
	createdAt, id, err := driver.DecodeJobCursor(driver.EncodeJobCursor(row))
	if err != nil || id != row.ID || !createdAt.Equal(row.CreatedAt) {
		t.Fatalf("created_at=%s id=%q err=%v", createdAt, id, err)
	}
	if _, _, err := driver.DecodeJobCursor("not-valid"); !errors.Is(err, driver.ErrInvalidCursor) {
		t.Fatalf("invalid cursor error=%v", err)
	}
	missingTimestamp := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"id":"job"}`))
	if _, _, err := driver.DecodeJobCursor(missingTimestamp); !errors.Is(err, driver.ErrInvalidCursor) {
		t.Fatalf("missing timestamp error=%v", err)
	}
}

func TestQueueCursorRoundTrip(t *testing.T) {
	row := driver.QueueRow{Name: "critical"}
	name, err := driver.DecodeQueueCursor(driver.EncodeQueueCursor(row))
	if err != nil || name != row.Name {
		t.Fatalf("name=%q err=%v", name, err)
	}
	invalidVersion := base64.RawURLEncoding.EncodeToString([]byte(`{"v":2,"name":"critical"}`))
	for _, cursor := range []string{"not-valid", invalidVersion} {
		if _, err := driver.DecodeQueueCursor(cursor); !errors.Is(err, driver.ErrInvalidCursor) {
			t.Fatalf("invalid cursor %q error=%v", cursor, err)
		}
	}
}

func FuzzDecodeJobCursor(f *testing.F) {
	f.Add("not-valid")
	f.Add(driver.EncodeJobCursor(driver.JobRow{ID: "job/42", CreatedAt: time.Unix(123, 456)}))
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"created_at":1,"id":"job"}`)))
	f.Fuzz(func(t *testing.T, cursor string) {
		createdAt, id, err := driver.DecodeJobCursor(cursor)
		if err != nil {
			if !errors.Is(err, driver.ErrInvalidCursor) {
				t.Fatalf("unclassified error: %v", err)
			}
			return
		}
		if createdAt.IsZero() || id == "" {
			t.Fatalf("accepted invalid cursor: created_at=%s id=%q", createdAt, id)
		}
		roundTripAt, roundTripID, roundTripErr := driver.DecodeJobCursor(driver.EncodeJobCursor(driver.JobRow{ID: id, CreatedAt: createdAt}))
		if roundTripErr != nil || roundTripID != id || !roundTripAt.Equal(createdAt) {
			t.Fatalf("round trip: created_at=%s id=%q err=%v", roundTripAt, roundTripID, roundTripErr)
		}
	})
}

func FuzzDecodeQueueCursor(f *testing.F) {
	f.Add("not-valid")
	f.Add(driver.EncodeQueueCursor(driver.QueueRow{Name: "critical"}))
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"name":"default"}`)))
	f.Fuzz(func(t *testing.T, cursor string) {
		name, err := driver.DecodeQueueCursor(cursor)
		if err != nil {
			if !errors.Is(err, driver.ErrInvalidCursor) {
				t.Fatalf("unclassified error: %v", err)
			}
			return
		}
		if name == "" {
			t.Fatal("accepted an empty queue name")
		}
		roundTripName, roundTripErr := driver.DecodeQueueCursor(driver.EncodeQueueCursor(driver.QueueRow{Name: name}))
		if roundTripErr != nil || roundTripName != name {
			t.Fatalf("round trip: name=%q err=%v", roundTripName, roundTripErr)
		}
	})
}
