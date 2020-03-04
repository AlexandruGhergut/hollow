package com.netflix.sunjeetsonboardingroot.resource.v1;

import com.amazonaws.services.route53.model.InvalidInputException;
import com.netflix.cinder.consumer.NFHollowBlobRetriever;
import com.netflix.cinder.util.GutenbergNamer;
import com.netflix.gutenberg.consume.DataVersionHistoryResponse;
import com.netflix.gutenberg.consumer.GutenbergFileConsumer;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer;
import com.netflix.sunjeetsonboardingroot.startup.JerseyModule;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(JerseyModule.CONSUME_A_NAMESPACE + "/" + "{namespace}")
public class ConsumeANamespace {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeANamespace.class);
    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response queryConsume(@PathParam("namespace") String namespace) throws Exception {
        Integer port = namespaceToPort.get(namespace);
        if (port == null) {
            throw new InvalidInputException("Invalid namespace; a Hollow consumer wasn't warmed up for this namespace");
        }

        URI uri = new URI("http://sunjeetsonboardingroot.cluster.us-east-1.test.cloud.netflix.net:" + port);
        return Response.temporaryRedirect(uri).build();
        // return Response.ok().build();
    }

    public static Map<String, Integer> namespaceToPort;

    public static void populateReadStates(List<String> namespaces, Map<String, Long> namespaceVersions) throws Exception {
        namespaceToPort = new HashMap<>();
        int incrementingPort = 50000;

        File blobCache = new File("/tmp/blob-cache");   // SNAP: change path
        boolean dirCreated = blobCache.mkdir();
        if (dirCreated == true) {
            logger.info("SNAP: Created directory " + blobCache.getPath());
        } else {
            logger.info("SNAP: Didn't need to create directory " + blobCache.getPath());
        }

        GutenbergFileConsumer gutenberg = GutenbergFileConsumer.localProxyForProdEnvironment();

        logger.info("SNAP: Starting explorers for configured namespaces");
        for (String namespace : namespaces) {
            logger.info("SNAP: Starting explorer for namespace " + namespace);
            HollowConsumer consumer = HollowConsumer.withBlobRetriever(new NFHollowBlobRetriever(gutenberg, namespace))
                    .withLocalBlobStore(blobCache)  // SNAP: local blob store is required for LZ4 decompression
                    .build();
            if (namespaceVersions.get(namespace) != null) {
                logger.info("SNAP: Refreshing namespace=" + namespace + " to configured version=" + namespaceVersions.get(namespace));
                consumer.triggerRefreshTo(namespaceVersions.get(namespace));
            } else {
                Optional<DataVersionHistoryResponse.HistoryItem> latest = gutenberg.getVersionHistory(GutenbergNamer.getSnapshotTopicName(namespace), 1).findFirst();
                if (latest.isPresent()) {
                    logger.info("SNAP: Refreshing namespace=" + namespace + " to latest snapshot version=" + latest.get().getVersion());
                    consumer.triggerRefreshTo(latest.get().getVersion());
                } else {
                    logger.warn("SNAP: Namespace=" + namespace + " didn't have any published snapshot versions");
                }
            }

            logger.info(
                    "SNAP: Cinder consumer for namespace " + namespace + " initialized to version " + consumer.getCurrentVersionId());

            // ServerSocket s = new ServerSocket(0);
            // int port = s.getLocalPort();    // SNAP: TODO: Change port
            int port= incrementingPort;

            // Explore
            logger.info("SNAP: Starting UI Server for namespace " + namespace + " on port " + port);

            Runnable task = () -> {
                String threadName = Thread.currentThread().getName();
                logger.info("SNAP: Successfully started explorer for namespace " + namespace + " port " + port + " thread " + threadName);
                try {
                    HollowExplorerUIServer uiServer = new HollowExplorerUIServer(consumer.getStateEngine(), port);
                    uiServer.start();
                } catch (Exception e) {
                    logger.error("SNAP: Exception starting explorer for namespace " + namespace + " on port " + port);
                }
            };
            task.run();

            namespaceToPort.put(namespace, port);
            incrementingPort ++;
        }
        logger.info("Done starting explorers for all namespaces, namespaceToPort= " + namespaceToPort.toString());
    }
}
