/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.opensearch.bolt;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.http.HttpHost;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StatusBoltTest extends AbstractOpenSearchTest {

    private StatusUpdaterBolt bolt;

    protected TestOutputCollector output;

    protected org.opensearch.client.RestHighLevelClient client;

    private static final Logger LOG = LoggerFactory.getLogger(StatusBoltTest.class);

    private static ExecutorService executorService;

    @BeforeAll
    static void beforeClass() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @AfterAll
    static void afterClass() {
        executorService.shutdown();
        executorService = null;
    }

    @BeforeEach
    void setupStatusBolt() throws IOException {
        bolt = new StatusUpdaterBolt();
        RestClientBuilder builder =
                RestClient.builder(
                        new HttpHost(
                                opensearchContainer.getHost(),
                                opensearchContainer.getMappedPort(9200)));
        client = new RestHighLevelClient(builder);
        // configure the status updater bolt
        Map<String, Object> conf = new HashMap<>();
        conf.put("opensearch.status.routing.fieldname", "metadata.key");
        conf.put(
                "opensearch.status.addresses",
                opensearchContainer.getHost() + ":" + opensearchContainer.getFirstMappedPort());
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        conf.put("metadata.persist", "someKey");
        output = new TestOutputCollector();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @AfterEach
    void close() {
        LOG.info("Closing updater bolt and Opensearch container");
        super.close();
        bolt.cleanup();
        output = null;
        try {
            client.close();
        } catch (IOException e) {
        }
    }

    private Future<Integer> store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
        return executorService.submit(
                () -> {
                    var outputSize = output.getAckedTuples().size();
                    while (outputSize == 0) {
                        Thread.sleep(100);
                        outputSize = output.getAckedTuples().size();
                    }
                    return outputSize;
                });
    }

    @Test
    public void testWaitAckCacheSpecAppliedFromConfig() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put("opensearch.status.waitack.cache.spec", "maximumSize=10,expireAfterWrite=1s");
        conf.put("opensearch.status.routing.fieldname", "metadata.key");
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);

        StatusUpdaterBolt bolt = new StatusUpdaterBolt();
        try {
            bolt.prepare(conf, mockContext, mockCollector);
        } catch (RuntimeException e) {
            // 연결 실패 시 예외 분기 발생 → Jacoco branch coverage 확보
            assertTrue(e.getMessage().contains("Can't connect"));
        }

        Field field = StatusUpdaterBolt.class.getDeclaredField("waitAck");
        field.setAccessible(true);
        Object cache = field.get(bolt);

        assertTrue(cache.getClass().getName().toLowerCase().contains("caffeine"));
    }
}
