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
