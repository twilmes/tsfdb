package com.experoinc.tsfdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author twilmes
 */
public class TsFDBVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(TsFDBVerticle.class);

    private Database db;
    private static DirectorySubspace rootLayer;
    private static DirectorySubspace rawSubspace;
    private static DirectorySubspace columnarSubspace;

    private HttpServer httpServer;
    private EventBus eb;
    private final WebClient client;

    private Schema schema;

    public static void main(String[] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();

        vertx.deployVerticle(new TsFDBVerticle(WebClient.create(vertx)));
    }

    public TsFDBVerticle(WebClient client) {
        super();
        this.client = client;
    }

    private void writeColumnar(final Transaction transaction, Long measurementId,
                               Map<Long, Map<Long, Long>> fieldData) {
        final Kryo kryo = new Kryo();
        final Set<Long> fieldIds = schema.getFields(measurementId);


    }
//
//    private byte[] compress(Kryo kryo, List<Long> data) {
//        final Output output = new Output(new ByteOutputStream());
//        output.writeVarInt(data.size(), true);
//        Long prev = null;
//        data.forEach(val -> {
//            if (prev == null) {
//                output.writeVarLong(val, true);
//            } else {
//                output.writeVarLong(val - prev, true);
//            }
//        });
//        return output.getBuffer();
//    }

    private List<Long> uncompress(Kryo kryo, byte[] data) {
        final Input input = new ByteBufferInput(data);
        int size = input.readVarInt(true);
        List<Long> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(input.readVarLong(true));
        }
        return result;
    }

    @Override
    public void start() throws Exception {
        final FDB fdb = FDB.selectAPIVersion(520);
        db = fdb.open();
        final Transaction tx = db.createTransaction();
        final DirectoryLayer  directoryLayer = new DirectoryLayer();
        rootLayer = directoryLayer.createOrOpen(tx, Arrays.asList("tsfdb")).get();
        rawSubspace = rootLayer.createOrOpen(tx, Arrays.asList("raw")).get();
        columnarSubspace = rootLayer.createOrOpen(tx, Arrays.asList("raw")).get();
        tx.commit();

        httpServer = vertx.createHttpServer();
        eb = vertx.eventBus();

        final InetAddress inetAddress = InetAddress.getLocalHost();
        final String hostAddress = inetAddress.getHostAddress();

        eb.consumer(hostAddress, message -> {
            System.out.println("Doit");
            message.reply(message.body());
        });

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route(HttpMethod.POST, "/series/:measurementId/:timestamp").handler(routingContext -> {
            final Long measurementId = Long.valueOf(routingContext.request().getParam("measurementId"));
            final Long timestamp = Long.valueOf(routingContext.request().getParam("timestamp"));
            final JsonObject fields = routingContext.getBodyAsJson();

            final Transaction transaction = db.createTransaction();
            fields.forEach(entry -> {
                final String fieldName = entry.getKey();
                final Long value = Long.valueOf((int)entry.getValue());
                final byte[] key = rootLayer.pack(Tuple.from(measurementId, fieldName, timestamp));
                transaction.set(key, Tuple.from(value).pack());
            });
            transaction.commit().whenComplete((result, e) ->
                    vertx.runOnContext(none -> routingContext.response().setStatusCode(200).end()));
        });

        router.route(HttpMethod.GET, "/series/:measurementId/:fieldName").handler(routingContext -> {
            final Long measurementId = Long.valueOf(routingContext.request().getParam("measurementId"));
            final String fieldName = routingContext.request().getParam("fieldName");
            final MultiMap queryParams = routingContext.queryParams();
            final Long startTstamp = Long.valueOf(queryParams.get("start"));
            final Long endTstamp = Long.valueOf(queryParams.get("end"));
            final boolean pushdown = Boolean.valueOf(queryParams.get("pushdown"));

            final Transaction transaction = db.createTransaction();
            byte[] startKey = rootLayer.pack(Tuple.from(measurementId, fieldName, startTstamp));
            byte[] endKey = rootLayer.pack(Tuple.from(measurementId, fieldName, endTstamp));


            if (pushdown) {
//                final CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(db, new byte[]{0}, new byte[]{(byte) 255});
//                CompletableFuture<List<byte[]>> collectedKeys = AsyncUtil.collectRemaining(boundaryKeys);
//                System.out.println("Pushing down predicate");
//                final List<byte[]> boundaries = collectedKeys.join();
//                boundaryKeys.close();
//                System.out.println("Boundaries: " + boundaries.toString());
//                final Iterator<byte[]> bytesIt = boundaries.iterator();
                final String[] addresses = LocalityUtil.getAddressesForKey(transaction, startKey).join();
                final String targetHost = addresses[0];
//                if (bytesIt.hasNext()) {
//                    Tuple startTuple = Tuple.from(bytesIt.next());
//                    System.out.println("Start for range: " + startTuple.toString());
//                final List<CompletableFuture<JsonObject>> pushdownFutures = new CopyOnWriteArrayList<>();
//                    while (bytesIt.hasNext()) {
//                        final byte[] endBytes = bytesIt.next();
//                        final Tuple endTuple = Tuple.from(endBytes);
//                try {
//                            final String[] targetHosts = LocalityUtil.getAddressesForKey(transaction, startTuple.pack()).get();
                    System.out.println("Routing query to: " +targetHost);
                CompletableFuture<JsonObject> future = new CompletableFuture<>();
                    client.get(9999, targetHost, "/series/" + measurementId + "/" + fieldName)
                            .addQueryParam("start", rootLayer.unpack(startKey).get(2).toString())
                            .addQueryParam("end", rootLayer.unpack(endKey).get(2).toString())
                            .addQueryParam("pushdown", "false")
                            .send(ar -> {

//                                pushdownFutures.add(future);
                                future.complete(ar.result().bodyAsJsonObject());
                            });
//                    routingContext.response().setStatusCode(200).end();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                }

//                CompletableFuture<List<JsonObject>> res = CompletableFuture.allOf(pushdownFutures.toArray(new CompletableFuture[pushdownFutures.size()]))
//                        .thenApply(v -> pushdownFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                future.whenComplete((jsonResults, ex) -> {
                    routingContext.response().setStatusCode(200).end(JsonObject.mapFrom(jsonResults).toString());
                });
                transaction.close();
//                } else {
//                    routingContext.response().setStatusCode(200).end("Boundaries not found");
//                }
            } else {
                transaction.getRange(startKey, endKey).asList().thenCombine(CompletableFuture.completedFuture(rootLayer), (result, dir) -> {
                    vertx.runOnContext(none -> {
                        final Map<Long, Long> responseMap = result.stream().collect(
                                Collectors.toMap(kv -> dir.unpack(kv.getKey()).getLong(2),
                                        kv -> Tuple.fromBytes(kv.getValue()).getLong(0)));
                        routingContext.response().setStatusCode(200).end(JsonObject.mapFrom(responseMap).toString());
                    });
                    return null;
                });
            }
        });

        httpServer.requestHandler(router::accept).listen(9999);
    }

    @Override
    public void stop() {
        db.close();
        httpServer.close();
        client.close();
    }
}
