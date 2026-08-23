package core_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kirimatt/goncordia/core"
)

type versionedArgs struct {
	FullName string `json:"full_name"`
}

func (versionedArgs) Kind() string { return "versioned" }

func TestRegistryUpcastsPayloadAndDiscardsIncompatibleVersions(t *testing.T) {
	registry := core.NewRegistry()
	var received *core.Job[versionedArgs]
	core.RegisterWorker(registry, core.WorkerFunc[versionedArgs](func(_ context.Context, job *core.Job[versionedArgs]) error {
		received = job
		return nil
	}), core.WorkerOpts{
		PayloadVersion: 2,
		Upcasters: map[int]core.PayloadUpcaster{
			1: func(payload json.RawMessage) (json.RawMessage, error) {
				var legacy struct {
					Name string `json:"name"`
				}
				if err := json.Unmarshal(payload, &legacy); err != nil {
					return nil, err
				}
				return json.Marshal(versionedArgs{FullName: legacy.Name})
			},
		},
	})
	raw := &core.RawJob{Kind: "versioned", Args: json.RawMessage(`{"name":"Ada"}`)}
	if err := registry.Process(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if received == nil || received.Args.FullName != "Ada" || received.PayloadVersion != 2 || raw.PayloadVersion != 2 {
		t.Fatalf("upcast result: received=%+v raw=%+v", received, raw)
	}

	future, err := core.EncodePayload(json.RawMessage(`{"full_name":"Grace"}`), 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Process(context.Background(), &core.RawJob{Kind: "versioned", Args: future}); err == nil {
		t.Fatal("future payload unexpectedly succeeded")
	} else if _, discard := core.AsDiscard(err); !discard {
		t.Fatalf("future payload error=%v, want discard directive", err)
	}
}

func TestRegistryTreatsMalformedPayloadAsPermanent(t *testing.T) {
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[versionedArgs](func(context.Context, *core.Job[versionedArgs]) error {
		return nil
	}), core.WorkerOpts{})
	err := registry.Process(context.Background(), &core.RawJob{Kind: "versioned", Args: json.RawMessage(`{"full_name":`)})
	if _, discard := core.AsDiscard(err); !discard {
		t.Fatalf("malformed payload error=%v, want discard directive", err)
	}
}

func FuzzDecodePayload(f *testing.F) {
	f.Add([]byte(`{"name":"legacy"}`))
	f.Add([]byte(`{"_goncordia_payload_version":2,"payload":{"full_name":"Ada"}}`))
	f.Add([]byte(`{"_goncordia_payload_version":0,"payload":{}}`))
	f.Add([]byte(`{`))
	f.Fuzz(func(t *testing.T, input []byte) {
		version, payload, err := core.DecodePayload(input)
		if err != nil {
			return
		}
		if version <= 0 || len(payload) == 0 || !json.Valid(payload) {
			t.Fatalf("invalid successful decode: version=%d payload=%q", version, payload)
		}
		encoded, err := core.EncodePayload(payload, version)
		if err != nil {
			t.Fatal(err)
		}
		roundTripVersion, roundTripPayload, err := core.DecodePayload(encoded)
		if err != nil || roundTripVersion != version || string(roundTripPayload) != string(payload) {
			t.Fatalf("round trip: version=%d payload=%q err=%v", roundTripVersion, roundTripPayload, err)
		}
	})
}
