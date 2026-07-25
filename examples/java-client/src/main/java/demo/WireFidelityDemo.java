package demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import turbovec.v1.AddRequest;
import turbovec.v1.AddResponse;
import turbovec.v1.CreateIndexRequest;
import turbovec.v1.DropIndexRequest;
import turbovec.v1.IndexKind;
import turbovec.v1.SearchRequest;
import turbovec.v1.TurboVecGrpc;

/**
 * Wire-fidelity demo for the turbovec-grpc server, companion to the
 * {@link TurboVecDemo} speed test.
 *
 * <p>Why the binary contract matters: protobuf carries {@code uint64} ids and
 * {@code float} coordinates exactly, while the JSON REST surface most vector
 * stores expose routes every number through a 64-bit double, which silently
 * rounds ids at or above 2^53 and makes float fidelity a serializer setting.
 *
 * <pre>
 *   mvn -q compile exec:java -Dexec.mainClass=demo.WireFidelityDemo
 *   mvn -q compile exec:java -Dexec.mainClass=demo.WireFidelityDemo -Dexec.args="256"   # dim
 *   TURBOVEC_GRPC_ADDR=host:port mvn -q compile exec:java -Dexec.mainClass=demo.WireFidelityDemo
 * </pre>
 */
public final class WireFidelityDemo {

    private static final String DEFAULT_ADDR = "127.0.0.1:50051";
    private static final int DEFAULT_DIM = 128;
    private static final int BIT_WIDTH = 4;

    private WireFidelityDemo() {}

    public static void main(String[] args) throws Exception {
        int dim = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_DIM;
        String addr = System.getenv().getOrDefault("TURBOVEC_GRPC_ADDR", DEFAULT_ADDR);

        ManagedChannel channel =
                Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
        try {
            System.out.printf("turbovec-grpc wire-fidelity demo — connected to %s%n", addr);
            fidelityDemo(channel, dim);
        } catch (StatusRuntimeException e) {
            System.err.printf(
                    "%nrpc failed: %s%nis the server up?  cargo run -p turbovec-grpc%n",
                    e.getStatus());
            System.exit(1);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * The same values, carried by protobuf versus routed through a JSON number
     * (an IEEE-754 double, which is what {@code JSON.parse} and every
     * double-backed REST client produce).
     */
    private static void fidelityDemo(ManagedChannel channel, int dim) {
        TurboVecGrpc.TurboVecBlockingStub blocking = TurboVecGrpc.newBlockingStub(channel);
        String indexId =
                blocking.createIndex(
                                CreateIndexRequest.newBuilder()
                                        .setDim(dim)
                                        .setBitWidth(BIT_WIDTH)
                                        .setKind(IndexKind.INDEX_KIND_ID_MAP)
                                        .setLazy(false)
                                        .build())
                        .getIndexId();

        // Ids chosen around the 2^53 mantissa limit of a 64-bit double, plus a
        // snowflake-scale id of the kind real systems mint.
        long[] ids = {42L, 1L << 53, (1L << 53) + 1, 1_861_392_837_450_923_417L};

        AddRequest.Builder add = AddRequest.newBuilder().setIndexId(indexId).setDim(dim);
        Random rng = new Random(11);
        for (long id : ids) {
            for (float v : TurboVecDemo.randomVector(dim, rng)) {
                add.addVectors(v);
            }
            add.addIds(id);
        }
        streamOne(channel, add.build());

        // One large id seen by three JVM client setups. Both JSON columns use a
        // real Jackson ObjectMapper on the same bytes; only the target type
        // differs. "double" is what JavaScript's JSON.parse and any double field
        // produce; "long" is a typed integer field, the correct JSON setup here.
        ObjectMapper json = new ObjectMapper();
        float[] probe = TurboVecDemo.randomVector(dim, new Random(5));
        System.out.printf("%n[1] one uint64 id, three client setups%n");
        System.out.printf(
                "    %-21s %-25s %-23s %-23s%n",
                "id", "JSON into a double", "JSON into a long", "gRPC uint64");
        for (long id : ids) {
            long viaDouble = jsonAsDouble(json, id);
            long viaLong = jsonAsLong(json, id);
            long viaGrpc = lookupOnly(blocking, indexId, probe, id);
            System.out.printf(
                    "    %-21d %-25s %-23s %-23s%n",
                    id,
                    viaDouble + (viaDouble == id ? " ok" : " LOST"),
                    viaLong + (viaLong == id ? " ok" : " LOST"),
                    viaGrpc + (viaGrpc == id ? " ok" : " LOST"));
        }
        System.out.printf(
                "    the digits survive as a long or a string; they round only when a client"
                        + " routes the number%n    through a double (JavaScript, Gson to Object, a"
                        + " double field). gRPC's uint64 removes the choice.%n");

        // The double path does not just lose a digit, it collides: two distinct
        // ids fold onto one number, so that client can no longer address them apart.
        long idA = ids[1]; // 2^53
        long idB = ids[2]; // 2^53 + 1, a different vector under a different id
        long idBAsDouble = jsonAsDouble(json, idB); // what the double path yields for B
        long askedViaDouble = lookupOnly(blocking, indexId, probe, idBAsDouble);
        long askedViaGrpc = lookupOnly(blocking, indexId, probe, idB);
        if (askedViaDouble != idA || askedViaGrpc != idB) {
            throw new IllegalStateException("id collision demo did not reproduce");
        }
        System.out.printf("%n[2] id collision on the double path%n");
        System.out.printf("    stored id A = %d and id B = %d, different vectors%n", idA, idB);
        System.out.printf("    through a double both become %d, so they are one number%n", idBAsDouble);
        System.out.printf(
                "    ask for B via a double     -> server returns %d  (that is A, not B)%n",
                askedViaDouble);
        System.out.printf(
                "    ask for B as a gRPC uint64 -> server returns %d  (correct)%n", askedViaGrpc);
        System.out.printf(
                "    a typed long or a string id keeps them apart too; gRPC removes the choice"
                        + " so no client can get it wrong.%n");

        System.out.printf("%n[3] float32 fidelity on the wire: protobuf vs a 6-digit JSON producer%n");
        System.out.printf(
                "    %-18s  %-18s  %-18s%n", "value", "protobuf wire", "JSON 6 sig-digits");
        float[] samples = {0.1f, 1e-7f, (float) Math.PI, 12345.678f, 0.036450123f};
        for (float f : samples) {
            float viaProto = protobufRoundTrip(f);
            float viaJson = jsonRoundTrip(f);
            System.out.printf(
                    "    %-18s  %-18s  %-18s%n",
                    f,
                    bitsMatch(f, viaProto) ? viaProto + "  ok" : viaProto + "  DRIFT",
                    bitsMatch(f, viaJson) ? viaJson + "  ok" : viaJson + "  DRIFT");
        }
        System.out.printf(
                "    (turbovec quantizes storage by design; the point here is the wire, not"
                        + " storage. Full-precision JSON can round-trip a float, but protobuf needs"
                        + " no such care.)%n");

        blocking.dropIndex(DropIndexRequest.newBuilder().setIndexId(indexId).build());
    }

    /** Send a single Add frame and wait for the summary. */
    private static void streamOne(ManagedChannel channel, AddRequest frame) {
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
        upload.onNext(frame);
        upload.onCompleted();
        done.join();
    }

    /** A float carried by the real generated message, byte-for-byte. */
    private static float protobufRoundTrip(float f) {
        try {
            byte[] wire = SearchRequest.newBuilder().addQueries(f).build().toByteArray();
            return SearchRequest.parseFrom(wire).getQueries(0);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A float printed by a producer that keeps 6 significant digits, then reparsed. */
    private static float jsonRoundTrip(float f) {
        return Float.parseFloat(String.format(Locale.ROOT, "%.6g", (double) f));
    }

    private static boolean bitsMatch(float a, float b) {
        return Float.floatToIntBits(a) == Float.floatToIntBits(b);
    }

    /** A large id routed through a JSON double, the JavaScript and double-field default. */
    private static long jsonAsDouble(ObjectMapper json, long id) {
        try {
            return (long) (double) json.readValue(Long.toString(id), Double.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The same id read into a typed long, the correct JSON setup on the JVM. */
    private static long jsonAsLong(ObjectMapper json, long id) {
        try {
            return json.readValue(Long.toString(id), Long.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Top-1 restricted to a single allowlisted id; returns the id the server yields. */
    private static long lookupOnly(
            TurboVecGrpc.TurboVecBlockingStub blocking, String indexId, float[] probe, long allowId) {
        return blocking.search(
                        SearchRequest.newBuilder()
                                .setIndexId(indexId)
                                .addAllQueries(TurboVecDemo.boxed(probe))
                                .setK(1)
                                .addAllowlist(allowId)
                                .build())
                .getResults(0)
                .getIds(0);
    }
}
