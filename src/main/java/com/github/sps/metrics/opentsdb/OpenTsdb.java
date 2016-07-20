/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sps.metrics.opentsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * OpenTSDB 2.0 REST client
 */
public class OpenTsdb {

    public static final int DEFAULT_BATCH_SIZE_LIMIT = 10;
    public static final int CONN_TIMEOUT_DEFAULT_MS = 5000;
    public static final int READ_TIMEOUT_DEFAULT_MS = 5000;
    private static final Logger logger = LoggerFactory.getLogger(OpenTsdb.class);

    /**
     * Initiate a client Builder with the provided base opentsdb server url.
     */
    public static Builder forService(String baseUrl) {
        return new Builder(baseUrl);
    }

    private final AsyncHttpClient.BoundRequestBuilder requestBuilder;
    private int batchSizeLimit = DEFAULT_BATCH_SIZE_LIMIT;

    private ObjectMapper mapper = new ObjectMapper();

    public static class Builder {
        private Integer connectionTimeout = CONN_TIMEOUT_DEFAULT_MS;
        private Integer readTimeout = READ_TIMEOUT_DEFAULT_MS;
        private String baseUrl;

        public Builder(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public Builder withConnectTimeout(Integer connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withReadTimeout(Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public OpenTsdb create() {
            return new OpenTsdb(baseUrl, connectionTimeout, readTimeout);
        }
    }

    private OpenTsdb(String baseURL, Integer connectionTimeout, Integer readTimeout) {

        AsyncHttpClientConfig cf = new AsyncHttpClientConfig.Builder()
                .setConnectTimeout(connectionTimeout)
                .setReadTimeout(readTimeout)
                .build();
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient(cf);

        this.requestBuilder = asyncHttpClient.preparePost(baseURL + "/api/put");
    }

    public void setBatchSizeLimit(int batchSizeLimit) {
        this.batchSizeLimit = batchSizeLimit;
    }

    /**
     * Send a metric to opentsdb
     */
    public void send(OpenTsdbMetric metric) {
        send(Collections.singleton(metric));
    }

    /**
     * send a set of metrics to opentsdb
     */
    public void send(Set<OpenTsdbMetric> metrics) {
        // we set the patch size because of existing issue in opentsdb where large batch of metrics failed
        // see at https://groups.google.com/forum/#!topic/opentsdb/U-0ak_v8qu0
        // we recommend batch size of 5 - 10 will be safer
        // alternatively you can enable chunked request
        if (batchSizeLimit > 0 && metrics.size() > batchSizeLimit) {
            final Set<OpenTsdbMetric> smallMetrics = new HashSet<OpenTsdbMetric>();
            for (final OpenTsdbMetric metric: metrics) {
                smallMetrics.add(metric);
                if (smallMetrics.size() >= batchSizeLimit) {
                    sendHelper(smallMetrics);
                    smallMetrics.clear();
                }
            }
            sendHelper(smallMetrics);
        } else {
            sendHelper(metrics);
        }
    }

    private void sendHelper(Set<OpenTsdbMetric> metrics) {
        if (!metrics.isEmpty()) {
            try {
                requestBuilder
                        .setBody(mapper.writeValueAsString(metrics))
                        .execute(new AsyncCompletionHandler<Void>() {
                            @Override
                            public Void onCompleted(Response response) throws Exception {
                                if (response.getStatusCode() != 204) {
                                    logger.error("send to opentsdb endpoint failed: ("
                                            + response.getStatusCode() + ") "
                                            + response.getResponseBody());
                                }
                                return null;
                            }
                        });
            } catch (Throwable ex) {
                logger.error("send to opentsdb endpoint failed", ex);
            }
        }
    }

}
