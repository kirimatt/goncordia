package driver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

type jobCursorV1 struct {
	Version   int    `json:"v"`
	CreatedAt int64  `json:"created_at"`
	ID        string `json:"id"`
}

type queueCursorV1 struct {
	Version int    `json:"v"`
	Name    string `json:"name"`
}

// EncodeJobCursor creates an opaque cursor for the row ordering
// created_at DESC, id DESC.
func EncodeJobCursor(row JobRow) string {
	payload, _ := json.Marshal(jobCursorV1{Version: 1, CreatedAt: row.CreatedAt.UTC().UnixNano(), ID: row.ID})
	return base64.RawURLEncoding.EncodeToString(payload)
}

// DecodeJobCursor validates and decodes an opaque job cursor.
func DecodeJobCursor(cursor string) (createdAt time.Time, id string, err error) {
	payload, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
	if decodeErr != nil {
		return time.Time{}, "", fmt.Errorf("%w: malformed encoding", ErrInvalidCursor)
	}
	var decoded jobCursorV1
	if jsonErr := json.Unmarshal(payload, &decoded); jsonErr != nil || decoded.Version != 1 || decoded.CreatedAt == 0 || decoded.ID == "" {
		return time.Time{}, "", fmt.Errorf("%w: unsupported or malformed payload", ErrInvalidCursor)
	}
	return time.Unix(0, decoded.CreatedAt).UTC(), decoded.ID, nil
}

// JobFollowsCursor reports whether row belongs after the cursor in descending
// created_at/id order.
func JobFollowsCursor(row JobRow, createdAt time.Time, id string) bool {
	return row.CreatedAt.Before(createdAt) || row.CreatedAt.Equal(createdAt) && row.ID < id
}

// JobPage is the stable administrative pagination envelope.
type JobPage struct {
	Items      []JobRow `json:"items"`
	NextCursor string   `json:"next_cursor,omitempty"`
	HasMore    bool     `json:"has_more"`
}

// EncodeQueueCursor creates an opaque cursor for queue-name ordering.
func EncodeQueueCursor(row QueueRow) string {
	payload, _ := json.Marshal(queueCursorV1{Version: 1, Name: row.Name})
	return base64.RawURLEncoding.EncodeToString(payload)
}

// DecodeQueueCursor validates and decodes an opaque queue cursor.
func DecodeQueueCursor(cursor string) (string, error) {
	payload, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
	if decodeErr != nil {
		return "", fmt.Errorf("%w: malformed encoding", ErrInvalidCursor)
	}
	var decoded queueCursorV1
	if jsonErr := json.Unmarshal(payload, &decoded); jsonErr != nil || decoded.Version != 1 || decoded.Name == "" {
		return "", fmt.Errorf("%w: unsupported or malformed payload", ErrInvalidCursor)
	}
	return decoded.Name, nil
}

// QueuePage is the stable administrative pagination envelope.
type QueuePage struct {
	Items      []*QueueRow `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
}
