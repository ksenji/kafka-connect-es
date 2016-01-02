package org.apache.kafka.connect.es;

import static org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig.ES_CLUSTER;
import static org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig.ES_CLUSTER_NAME;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.es.dcl.Factory;
import org.apache.kafka.connect.es.dcl.Factory.Dcl;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ElasticSearchClient {

    private ElasticSearchSinkConnectorConfig config;

    public ElasticSearchClient(ElasticSearchSinkConnectorConfig config) {
        this.config = config;
    }

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchClient.class);

    private TransportAddress[] toTransportAddresses(List<String> nodes) {
        List<TransportAddress> addresses = new ArrayList<>(nodes.size());
        for (String node : nodes) {
            String[] hostAndPort = node.split(":");

            String host = null;
            int port = 9300;

            if (hostAndPort.length >= 1) {
                host = hostAndPort[0];
            }

            if (hostAndPort.length >= 2) {
                try {
                    port = Integer.parseInt(hostAndPort[1]);
                } catch (NumberFormatException e) {
                    port = -1;
                    if (log.isInfoEnabled()) {
                        log.info("Ingoring invalid port: {}", port);
                    }
                }
            }
            if (port != -1) {
                try {
                    addresses.add(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                } catch (UnknownHostException e) {
                    if (log.isErrorEnabled()) {
                        log.error("Unknow host, Ingoring this node", e);
                    }
                }
            }
        }
        return addresses.toArray(new TransportAddress[addresses.size()]);
    }

    private Dcl<Client> client = Factory.of(() -> {
        Settings settings = Settings.settingsBuilder().put("cluster.name", config.getString(ES_CLUSTER_NAME)).build();
        //@formatter:off
        return TransportClient.builder()
                              .settings(settings)
                              .build()
                              .addTransportAddresses(toTransportAddresses(config.getList(ES_CLUSTER)));
        //@formatter:on
    });

    private AtomicInteger counter = new AtomicInteger();
    private Semaphore semaphore = new Semaphore(1);
    private List<ActionRequest> failedRequests = Collections.synchronizedList(new ArrayList<>());

    private Dcl<BulkProcessor> processor = Factory.of(() -> {
        Builder builder = BulkProcessor.builder(client.get(), new Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                semaphore.acquireUninterruptibly();
            }

            private List<ActionRequest> requestsOf(BulkRequest request) {
                return request.requests();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response != null) {
                    List<ActionRequest> requests = requestsOf(request);
                    BulkItemResponse[] items = response.getItems();

                    for (int i = 0; i < items.length; i++) {
                        BulkItemResponse item = items[i];
                        if (item.isFailed()) {
                            failedRequests.add(requests.get(i));
                        } else {
                            counter.decrementAndGet();
                        }
                    }
                }
                semaphore.release();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (failure != null) {
                    if (log.isErrorEnabled()) {
                        log.error("Failed to execute BulkRequest", failure);
                    }
                }
                failedRequests.addAll(requestsOf(request));
                semaphore.release();
            }
        });

        return builder.setConcurrentRequests(1).build();
    });

    Client getClient() {
        return client.get();
    }

    BulkProcessor getBulkProcessor() {
        return processor.get();
    }

    void updatePendingRequests(int size) {
        boolean success = false;
        while (!success) {
            int expect = counter.get();
            success = counter.compareAndSet(expect, expect + size);
        }
    }

    void waitUntilNonePending() {
        while (true) {
            /* ensure no requests are pending */
            semaphore.acquireUninterruptibly();
            if (counter.get() > 0) {
                List<ActionRequest> requests = null;
                try {
                    /*
                     * copy failed requests to local variable & clear
                     * failedRequests
                     */
                    requests = new ArrayList<ActionRequest>(failedRequests);
                    failedRequests.clear();
                } finally {
                    semaphore.release();
                }
                if (!(requests == null || requests.isEmpty())) {
                    BulkProcessor bulkProcessor = getBulkProcessor();
                    for (ActionRequest request : requests) {
                        bulkProcessor.add(request);
                    }
                    bulkProcessor.flush();
                }
            } else {
                semaphore.release();
                break;
            }
        }
    }

    void stop() {
        waitUntilNonePending();
        processor.get().close();
        client.get().close();
    }
}
