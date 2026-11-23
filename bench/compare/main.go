// Command compare benchmarks goncordia against river (Postgres), asynq and
// machinery (Redis) with the same workloads on the same servers.
//
//	go run . -pg postgres://... -redis localhost:6379 -systems all
//
// Every system runs with its default polling and fetch settings; only the
// worker concurrency is set. Each scenario starts from an empty database.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// system is one queue library wired to one backend.
type system interface {
	Name() string
	// Reset empties the backend and prepares the schema.
	Reset(ctx context.Context) error
	// Enqueue inserts one job.
	Enqueue(ctx context.Context, n int) error
	// EnqueueBatch inserts len(ns) jobs in one call; ok=false if the library
	// has no batch insert.
	EnqueueBatch(ctx context.Context, ns []int) (ok bool, err error)
	// Start runs workers with the given concurrency; handled is called once
	// per executed job with the job's payload.
	Start(ctx context.Context, concurrency int, handled func(n int)) (stop func(), err error)
	Close()
}

type result struct {
	system, scenario string
	value            string
}

func main() {
	pgDSN := flag.String("pg", "", "Postgres DSN")
	redisAddr := flag.String("redis", "", "Redis address")
	systems := flag.String("systems", "all", "comma-separated systems or all")
	jobs := flag.Int("jobs", 20000, "jobs for the drain scenario")
	enqueueN := flag.Int("enqueue", 5000, "jobs for the single-insert scenario")
	latencyN := flag.Int("latency", 200, "jobs for the latency scenario")
	flag.Parse()
	ctx := context.Background()

	var all []system
	add := func(s system, err error) {
		if err != nil {
			fmt.Fprintln(os.Stderr, "setup:", err)
			os.Exit(1)
		}
		all = append(all, s)
	}
	want := func(name string) bool {
		return *systems == "all" || slices.Contains(strings.Split(*systems, ","), name)
	}
	if *pgDSN != "" {
		if want("goncordia-pgxv5") {
			add(newGoncordiaPG(ctx, *pgDSN))
		}
		if want("river") {
			add(newRiver(ctx, *pgDSN, false))
		}
		if want("river-tuned") {
			add(newRiver(ctx, *pgDSN, true))
		}
	}
	if *redisAddr != "" {
		if want("goncordia-redis") {
			add(newGoncordiaRedis(ctx, *redisAddr))
		}
		if want("asynq") {
			add(newAsynq(*redisAddr, false))
		}
		if want("asynq-tuned") {
			add(newAsynq(*redisAddr, true))
		}
		if want("machinery") {
			add(newMachinery(*redisAddr))
		}
	}

	var results []result
	record := func(s system, scenario, value string) {
		fmt.Printf("%-16s %-28s %s\n", s.Name(), scenario, value)
		results = append(results, result{s.Name(), scenario, value})
	}
	for _, s := range all {
		must(s.Reset(ctx))
		start := time.Now()
		for i := range *enqueueN {
			must(s.Enqueue(ctx, i))
		}
		record(s, "enqueue single", rate(*enqueueN, time.Since(start)))

		must(s.Reset(ctx))
		batch := make([]int, 100)
		start = time.Now()
		ok := true
		for i := 0; i < *enqueueN && ok; i += 100 {
			for k := range batch {
				batch[k] = i + k
			}
			var err error
			ok, err = s.EnqueueBatch(ctx, batch)
			must(err)
		}
		if ok {
			record(s, "enqueue batch x100", rate(*enqueueN, time.Since(start)))
		} else {
			record(s, "enqueue batch x100", "no batch API")
		}

		for _, c := range []int{10, 100} {
			must(s.Reset(ctx))
			preload(ctx, s, *jobs)
			var done, dups atomic.Int64
			seen := make([]atomic.Bool, *jobs)
			finished := make(chan struct{})
			start = time.Now()
			stop, err := s.Start(ctx, c, func(n int) {
				if n < 0 || n >= len(seen) || seen[n].Swap(true) {
					dups.Add(1)
					return
				}
				if done.Add(1) == int64(*jobs) {
					close(finished)
				}
			})
			must(err)
			select {
			case <-finished:
				elapsed := time.Since(start)
				time.Sleep(500 * time.Millisecond) // catch late duplicates
				value := rate(*jobs, elapsed)
				if d := dups.Load(); d > 0 {
					value += fmt.Sprintf(", %d DUPLICATE executions", d)
				}
				record(s, fmt.Sprintf("drain %d jobs c=%d", *jobs, c), value)
			case <-time.After(5 * time.Minute):
				record(s, fmt.Sprintf("drain %d jobs c=%d", *jobs, c), fmt.Sprintf("timeout after 5m (%d done)", done.Load()))
			}
			stop()
		}

		must(s.Reset(ctx))
		got := make(chan int, 1)
		stop, err := s.Start(ctx, 10, func(n int) { got <- n })
		must(err)
		time.Sleep(2 * time.Second) // let workers reach their idle wait
		lat := make([]time.Duration, 0, *latencyN)
		for i := range *latencyN {
			t := time.Now()
			must(s.Enqueue(ctx, i))
			select {
			case <-got:
				lat = append(lat, time.Since(t))
			case <-time.After(30 * time.Second):
				fmt.Fprintln(os.Stderr, s.Name(), "latency job lost")
			}
		}
		stop()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		if len(lat) > 0 {
			record(s, "idle pickup latency", fmt.Sprintf("p50 %s  p99 %s",
				lat[len(lat)/2].Round(100*time.Microsecond), lat[len(lat)*99/100].Round(100*time.Microsecond)))
		}
		s.Close()
	}

	fmt.Println()
	fmt.Println("| system | scenario | result |")
	fmt.Println("|---|---|---|")
	for _, r := range results {
		fmt.Printf("| %s | %s | %s |\n", r.system, r.scenario, r.value)
	}
}

// preload inserts n jobs, in batches when the library supports it.
func preload(ctx context.Context, s system, n int) {
	batch := make([]int, 0, 500)
	for i := 0; i < n; i += 500 {
		batch = batch[:0]
		for k := i; k < min(i+500, n); k++ {
			batch = append(batch, k)
		}
		ok, err := s.EnqueueBatch(ctx, batch)
		must(err)
		if !ok {
			for _, k := range batch {
				must(s.Enqueue(ctx, k))
			}
		}
	}
}

func rate(n int, d time.Duration) string {
	return fmt.Sprintf("%.0f jobs/s (%s)", float64(n)/d.Seconds(), d.Round(time.Millisecond))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
