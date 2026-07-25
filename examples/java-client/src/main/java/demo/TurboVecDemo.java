package demo;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import turbovec.v1.AddRequest;
import turbovec.v1.AddResponse;
import turbovec.v1.CreateIndexRequest;
import turbovec.v1.CreateIndexResponse;
import turbovec.v1.DropIndexRequest;
import turbovec.v1.GetIndexInfoRequest;
import turbovec.v1.IndexKind;
import turbovec.v1.QueryResult;
import turbovec.v1.SearchRequest;
import turbovec.v1.SearchResponse;
import turbovec.v1.TurboVecGrpc;

/**
 * Java demo client for the turbovec-grpc server: the speed test.
 *
 * <p>Build an index by client-streaming vectors in, then time top-k queries
 * and report ingest throughput and query latency (p50/p95/p99), all measured
 * from the JVM. Defaults match the TypeScript, Python, Go, and Rust examples,
 * so numbers are comparable across languages.
 *
 * <p>For the wire-fidelity demonstration (uint64 ids vs JSON doubles, float32
 * vs 6-digit JSON) see {@link WireFidelityDemo}.
 *
 * <pre>
 *   mvn -q compile exec:java
 *   mvn -q compile exec:java -Dexec.args="100000 768 2000"
 *   TURBOVEC_GRPC_ADDR=host:port mvn -q compile exec:java
 * </pre>
 */
public final class TurboVecDemo {

    private static final String DEFAULT_ADDR = "127.0.0.1:50051";

    private static final int DEFAULT_VECTORS = 20_000;
    private static final int DEFAULT_DIM = 128;
    private static final int DEFAULT_QUERIES = 500;

    private static final int BIT_WIDTH = 4;
    private static final int TOP_K = 10;
    private static final int WARMUP_QUERIES = 50;

    private TurboVecDemo() {}

    public static void main(String[] args) throws Exception {
        int nVectors = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_VECTORS;
        int dim = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_DIM;
        int nQueries = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_QUERIES;
        String addr = System.getenv().getOrDefault("TURBOVEC_GRPC_ADDR", DEFAULT_ADDR);

        ManagedChannel channel =
                Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
        try {
            System.out.printf("turbovec-grpc demo — connected to %s%n", addr);
            speedDemo(channel, nVectors, dim, nQueries);
        } catch (StatusRuntimeException e) {
            System.err.printf(
                    "%nrpc failed: %s%nis the server up?  cargo run -p turbovec-grpc%n",
                    e.getStatus());
            System.exit(1);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Build an index from the JVM, then time ingest and queries. */
    private static void speedDemo(ManagedChannel channel, int nVectors, int dim, int nQueries) {
        TurboVecGrpc.TurboVecBlockingStub blocking = TurboVecGrpc.newBlockingStub(channel);

        CreateIndexResponse created =
                blocking.createIndex(
                        CreateIndexRequest.newBuilder()
                                .setDim(dim)
                                .setBitWidth(BIT_WIDTH)
                                .setKind(IndexKind.INDEX_KIND_ID_MAP)
                                .setLazy(false)
                                .build());
        String indexId = created.getIndexId();

        System.out.printf(
                "%n[1] indexing %,d vectors of dim %d at %d-bit%n", nVectors, dim, BIT_WIDTH);
        long ingestStart = System.nanoTime();
        long added = streamVectors(channel, indexId, nVectors, dim);
        double ingestSecs = (System.nanoTime() - ingestStart) / 1e9;
        double wireMb = (double) nVectors * dim * Float.BYTES / 1e6;
        System.out.printf(
                "    added %,d in %.2fs  =  %,.0f vectors/sec  (%,.0f MB of raw float32 sent)%n",
                added, ingestSecs, added / ingestSecs, wireMb);

        long len =
                blocking.getIndexInfo(
                                GetIndexInfoRequest.newBuilder().setIndexId(indexId).build())
                        .getLen();
        System.out.printf("    server reports %,d vectors in the index%n", len);

        System.out.printf("%n[2] top-%d search, one query at a time%n", TOP_K);
        Random rng = new Random(1234);
        for (int i = 0; i < WARMUP_QUERIES; i++) {
            blocking.search(searchRequest(indexId, randomVector(dim, rng), TOP_K));
        }
        // Query vectors are generated outside the timed region, so latency and
        // served QPS reflect the round trip and the server scan, not the client.
        long[] latenciesNs = new long[nQueries];
        long totalNs = 0;
        for (int i = 0; i < nQueries; i++) {
            SearchRequest req = searchRequest(indexId, randomVector(dim, rng), TOP_K);
            long start = System.nanoTime();
            SearchResponse resp = blocking.search(req);
            long elapsed = System.nanoTime() - start;
            latenciesNs[i] = elapsed;
            totalNs += elapsed;
            if (resp.getResults(0).getIdsCount() != TOP_K) {
                throw new IllegalStateException("expected " + TOP_K + " neighbours");
            }
        }
        Arrays.sort(latenciesNs);
        System.out.printf(
                "    %,d queries  =  %,.0f queries/sec served (single client thread)%n",
                nQueries, nQueries / (totalNs / 1e9));
        System.out.printf(
                "    latency  p50 %.2f ms   p95 %.2f ms   p99 %.2f ms%n",
                percentileMs(latenciesNs, 50), percentileMs(latenciesNs, 95),
                percentileMs(latenciesNs, 99));

        // The server-streaming variant: one QueryResult per query, in order.
        System.out.printf("%n[3] server-streaming search, batch of 4 queries%n");
        SearchRequest batch = searchRequest(indexId, randomVectors(dim, 4, rng), TOP_K);
        Iterator<QueryResult> stream = blocking.searchStream(batch);
        int streamed = 0;
        while (stream.hasNext()) {
            QueryResult qr = stream.next();
            System.out.printf(
                    "    query %d -> %d neighbours, best score %.4f%n",
                    streamed, qr.getIdsCount(), qr.getScores(0));
            streamed++;
        }

        blocking.dropIndex(DropIndexRequest.newBuilder().setIndexId(indexId).build());
    }

    /**
     * Client-streaming Add. Vectors are generated frame by frame and never all
     * held in memory at once. Frames stay under the server's 4 MB message limit.
     */
    private static long streamVectors(ManagedChannel channel, String indexId, int nVectors, int dim) {
        int perFrame = Math.max(1, 3_000_000 / (dim * Float.BYTES));
        CompletableFuture<AddResponse> done = new CompletableFuture<>();
        StreamObserver<AddRequest> upload =
                TurboVecGrpc.newStub(channel)
                        .add(
                                new StreamObserver<>() {
                                    @Override
                                    public void onNext(AddResponse value) {
                                        done.complete(value);
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        done.completeExceptionally(t);
                                    }

                                    @Override
                                    public void onCompleted() {}
                                });

        Random rng = new Random(7);
        int sent = 0;
        while (sent < nVectors) {
            int rows = Math.min(perFrame, nVectors - sent);
            AddRequest.Builder frame = AddRequest.newBuilder().setIndexId(indexId).setDim(dim);
            for (int r = 0; r < rows; r++) {
                for (int d = 0; d < dim; d++) {
                    frame.addVectors(rng.nextFloat() * 2f - 1f);
                }
                frame.addIds(sent + r);
            }
            upload.onNext(frame.build());
            sent += rows;
        }
        upload.onCompleted();
        return done.join().getAdded();
    }

    static SearchRequest searchRequest(String indexId, float[] queries, int k) {
        return SearchRequest.newBuilder()
                .setIndexId(indexId)
                .addAllQueries(boxed(queries))
                .setK(k)
                .build();
    }

    static float[] randomVector(int dim, Random rng) {
        return randomVectors(dim, 1, rng);
    }

    static float[] randomVectors(int dim, int count, Random rng) {
        float[] out = new float[dim * count];
        for (int i = 0; i < out.length; i++) {
            out[i] = rng.nextFloat() * 2f - 1f;
        }
        return out;
    }

    static List<Float> boxed(float[] values) {
        List<Float> out = new ArrayList<>(values.length);
        for (float v : values) {
            out.add(v);
        }
        return out;
    }

    private static double percentileMs(long[] sortedNs, int percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedNs.length) - 1;
        index = Math.min(Math.max(index, 0), sortedNs.length - 1);
        return sortedNs[index] / 1e6;
    }
}
