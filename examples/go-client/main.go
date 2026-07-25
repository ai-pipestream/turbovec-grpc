// Go demo client for the turbovec-grpc server.
//
// The index is built and queried entirely from Go over gRPC: client-stream a
// corpus in, then time top-k search. You get ingest throughput and query
// latency (p50/p95/p99) measured from the client. protobuf carries uint64 ids
// and float coordinates exactly; Go's uint64 is exact end to end.
//
//	go run .
//	go run . 1000000 768 2000
//	TURBOVEC_GRPC_ADDR=host:port go run .
package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "turbovec-go-client/gen/turbovec/v1"
)

const (
	defaultAddr    = "127.0.0.1:50051"
	defaultVectors = 20_000
	defaultDim     = 128
	defaultQueries = 500

	bitWidth = 4
	topK     = 10
)

func main() {
	nVectors := argInt(0, defaultVectors)
	dim := argInt(1, defaultDim)
	nQueries := argInt(2, defaultQueries)
	addr := os.Getenv("TURBOVEC_GRPC_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	client := pb.NewTurboVecClient(conn)
	ctx := context.Background()

	fmt.Printf("turbovec-grpc demo — connected to %s\n", addr)

	created, err := client.CreateIndex(ctx, &pb.CreateIndexRequest{
		Dim:      uint32(dim),
		BitWidth: bitWidth,
		Kind:     pb.IndexKind_INDEX_KIND_ID_MAP,
	})
	must("is the server up?  cargo run -p turbovec-grpc", err)
	indexID := created.GetIndexId()

	fmt.Printf("\n[1] indexing %d vectors of dim %d at %d-bit\n", nVectors, dim, bitWidth)
	start := time.Now()
	added := streamVectors(ctx, client, indexID, nVectors, dim)
	ingest := time.Since(start).Seconds()
	wireMb := float64(nVectors*dim*4) / 1e6
	fmt.Printf("    added %d in %.2fs  =  %.0f vectors/sec  (%.0f MB of raw float32 sent)\n",
		added, ingest, float64(added)/ingest, wireMb)

	info, err := client.GetIndexInfo(ctx, &pb.GetIndexInfoRequest{IndexId: indexID})
	must("", err)
	fmt.Printf("    server reports %d vectors in the index\n", info.GetLen())

	// Query vectors are generated outside the timed region, so latency and
	// served QPS reflect the round trip and the server scan, not the client.
	fmt.Printf("\n[2] top-%d search, one query at a time\n", topK)
	rng := rand.New(rand.NewSource(1234))
	latencies := make([]time.Duration, 0, nQueries)
	total := time.Duration(0)
	for i := 0; i < nQueries; i++ {
		req := &pb.SearchRequest{IndexId: indexID, Queries: randomVectors(dim, 1, rng), K: topK}
		t0 := time.Now()
		resp, err := client.Search(ctx, req)
		must("", err)
		elapsed := time.Since(t0)
		latencies = append(latencies, elapsed)
		total += elapsed
		if len(resp.GetResults()[0].GetIds()) != topK {
			fatal(fmt.Errorf("expected %d neighbours", topK))
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("    %d queries  =  %.0f queries/sec served (single client goroutine)\n",
		nQueries, float64(nQueries)/total.Seconds())
	fmt.Printf("    latency  p50 %.2f ms   p95 %.2f ms   p99 %.2f ms\n",
		percentileMs(latencies, 50), percentileMs(latencies, 95), percentileMs(latencies, 99))

	// The server-streaming variant: neighbours arrive one query at a time.
	fmt.Printf("\n[3] server-streaming search, batch of 4 queries\n")
	stream, err := client.SearchStream(ctx, &pb.SearchRequest{
		IndexId: indexID, Queries: randomVectors(dim, 4, rng), K: topK,
	})
	must("", err)
	for q := 0; ; q++ {
		qr, err := stream.Recv()
		if err == io.EOF {
			break
		}
		must("", err)
		fmt.Printf("    query %d -> %d neighbours, best score %.4f\n", q, len(qr.GetIds()), qr.GetScores()[0])
	}

	// A snowflake-scale id, round-tripped through an allowlist lookup.
	bigID := uint64(1_861_392_837_450_923_417)
	addOne(ctx, client, indexID, dim, bigID, rand.New(rand.NewSource(11)))
	one, err := client.Search(ctx, &pb.SearchRequest{
		IndexId: indexID, Queries: randomVectors(dim, 1, rng), K: 1, Allowlist: []uint64{bigID},
	})
	must("", err)
	fmt.Printf("\n[4] uint64 ids: Go's uint64 is exact end to end — stored %d, server returned %d\n",
		bigID, one.GetResults()[0].GetIds()[0])

	if _, err := client.DropIndex(ctx, &pb.DropIndexRequest{IndexId: indexID}); err != nil {
		fatal(err)
	}
}

// Client-streaming Add. Vectors are generated frame by frame and never all
// held in memory at once. Frames stay under the server's 4 MB message limit.
func streamVectors(ctx context.Context, client pb.TurboVecClient, indexID string, nVectors, dim int) uint64 {
	perFrame := max(1, 3_000_000/(dim*4))
	upload, err := client.Add(ctx)
	must("", err)

	rng := rand.New(rand.NewSource(7))
	for sent := 0; sent < nVectors; {
		rows := min(perFrame, nVectors-sent)
		frame := &pb.AddRequest{
			IndexId: indexID,
			Dim:     uint32(dim),
			Vectors: randomVectors(dim, rows, rng),
			Ids:     make([]uint64, rows),
		}
		for r := range rows {
			frame.Ids[r] = uint64(sent + r)
		}
		must("", upload.Send(frame))
		sent += rows
	}
	resp, err := upload.CloseAndRecv()
	must("", err)
	return resp.GetAdded()
}

// addOne uploads a single vector under one external id.
func addOne(ctx context.Context, client pb.TurboVecClient, indexID string, dim int, id uint64, rng *rand.Rand) {
	upload, err := client.Add(ctx)
	must("", err)
	must("", upload.Send(&pb.AddRequest{
		IndexId: indexID, Dim: uint32(dim), Vectors: randomVectors(dim, 1, rng), Ids: []uint64{id},
	}))
	_, err = upload.CloseAndRecv()
	must("", err)
}

func randomVectors(dim, count int, rng *rand.Rand) []float32 {
	out := make([]float32, dim*count)
	for i := range out {
		out[i] = rng.Float32()*2 - 1
	}
	return out
}

func percentileMs(sorted []time.Duration, p int) float64 {
	i := max(0, min(int(float64(p)/100*float64(len(sorted))+0.5), len(sorted)-1))
	return float64(sorted[i]) / 1e6
}

func argInt(i, fallback int) int {
	if len(os.Args) > i+1 {
		if v, err := strconv.Atoi(os.Args[i+1]); err == nil {
			return v
		}
	}
	return fallback
}

func must(hint string, err error) {
	if err != nil {
		if hint != "" {
			fmt.Fprintf(os.Stderr, "\nrpc failed: %v\n%s\n", err, hint)
		} else {
			fmt.Fprintf(os.Stderr, "\nrpc failed: %v\n", err)
		}
		os.Exit(1)
	}
}

func fatal(err error) { must("", err) }
