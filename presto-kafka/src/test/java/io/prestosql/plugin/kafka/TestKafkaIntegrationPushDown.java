/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.ResultWithQueryId;
import io.prestosql.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.prestosql.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestKafkaIntegrationPushDown
        extends AbstractTestQueryFramework
{
    private static final int MESSAGE_NUM = 1000;
    private static final int TIMESTAMP_TEST_COUNT = 5;
    private static final int TIMESTAMP_TEST_START_INDEX = 2;
    private static final int TIMESTAMP_TEST_END_INDEX = 4;

    private TestingKafka testingKafka;
    private String topicNamePartition;
    private String topicNameOffset;
    private String topicNameCreateTime;
    private String topicNameLogAppend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new TestingKafka();
        topicNamePartition = "test_push_down_partition_" + UUID.randomUUID().toString().replaceAll("-", "_");
        topicNameOffset = "test_push_down_offset_" + UUID.randomUUID().toString().replaceAll("-", "_");
        topicNameCreateTime = "test_push_down_create_time_" + UUID.randomUUID().toString().replaceAll("-", "_");
        topicNameLogAppend = "test_push_down_log_append_" + UUID.randomUUID().toString().replaceAll("-", "_");

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicNamePartition, new SchemaTableName("default", topicNamePartition)))
                        .put(createEmptyTopicDescription(topicNameOffset, new SchemaTableName("default", topicNameOffset)))
                        .put(createEmptyTopicDescription(topicNameCreateTime, new SchemaTableName("default", topicNameCreateTime)))
                        .put(createEmptyTopicDescription(topicNameLogAppend, new SchemaTableName("default", topicNameLogAppend)))
                        .build())
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.messages-per-split", "100")
                        .build())
                .build();
        testingKafka.createTopicWithConfig(2, 1, topicNamePartition, false);
        testingKafka.createTopicWithConfig(2, 1, topicNameOffset, false);
        testingKafka.createTopicWithConfig(2, 1, topicNameCreateTime, false);
        testingKafka.createTopicWithConfig(2, 1, topicNameLogAppend, true);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
    {
        if (testingKafka != null) {
            testingKafka.close();
            testingKafka = null;
        }
    }

    @Test
    public void testPartitionPushDown() throws ExecutionException, InterruptedException
    {
        createMessages(topicNamePartition);
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        String sql = String.format("SELECT count(*) FROM default.%s WHERE _partition_id=1",
                topicNamePartition);

        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), MESSAGE_NUM / 2);
    }

    @Test
    public void testOffsetPushDown() throws ExecutionException, InterruptedException
    {
        createMessages(topicNameOffset);
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        String sql = String.format("SELECT count(*) FROM default.%s WHERE _partition_offset between 2 and 10",
                topicNameOffset);

        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 18);

        sql = String.format("SELECT count(*) FROM default.%s WHERE _partition_offset > 2 and _partition_offset < 10",
                topicNameOffset);

        queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 14);

        sql = String.format("SELECT count(*) FROM default.%s WHERE _partition_offset = 3",
                topicNameOffset);

        queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 2);
    }

    @Test
    public void testTimestampCreateTimeModePushDown() throws ExecutionException, InterruptedException
    {
        Pair<String, String> timePair = createTimestampTestMessages(topicNameCreateTime);
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        // ">= startTime" insure including index 2, "< endTime"  insure excluding index 4;
        String sql = String.format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'",
                topicNameCreateTime, timePair.first(), timePair.second());

        // timestamp_upper_bound_force_push_down_enabled default as false.
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 998);

        // timestamp_upper_bound_force_push_down_enabled set as true.
        Session sessionWithUpperBoundPushDownEnabled = Session.builder(getSession())
                .setSystemProperty("kafka.timestamp_upper_bound_force_push_down_enabled", "true")
                .build();

        queryResult = queryRunner.executeWithQueryId(sessionWithUpperBoundPushDownEnabled, sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 2);
    }

    @Test
    public void testTimestampLogAppendModePushDown() throws ExecutionException, InterruptedException
    {
        Pair<String, String> timePair = createTimestampTestMessages(topicNameLogAppend);
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        // ">= startTime" insure including index 2, "< endTime"  insure excluding index 4;
        String sql = String.format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'",
                topicNameLogAppend, timePair.first(), timePair.second());
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 2);
    }

    private QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, ResultWithQueryId<MaterializedResult> queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.getQueryId());
    }

    private Pair<String, String> createTimestampTestMessages(String topicName) throws ExecutionException, InterruptedException
    {
        String startTime = null;
        String endTime = null;
        Future<RecordMetadata> lastSendFuture = Futures.immediateFuture(null);
        long lastTimeStamp = -1;
        try (KafkaProducer<Long, Object> producer = testingKafka.createProducer()) {
            for (long messageNum = 0; messageNum < MESSAGE_NUM; messageNum++) {
                long key = messageNum;
                long value = messageNum;
                lastSendFuture = producer.send(new ProducerRecord<>(topicName, key, value));
                // Record timestamp to build expected timestamp
                if (messageNum < TIMESTAMP_TEST_COUNT) {
                    RecordMetadata r = lastSendFuture.get();
                    assertTrue(lastTimeStamp != r.timestamp());
                    lastTimeStamp = r.timestamp();
                    if (messageNum == TIMESTAMP_TEST_START_INDEX) {
                        startTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(r.timestamp()), ZoneId.of("UTC")));
                    }
                    else if (messageNum == TIMESTAMP_TEST_END_INDEX) {
                        endTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(r.timestamp()), ZoneId.of("UTC")));
                    }
                    // Sleep for a while to ensure different timestamps for different messages..
                    Thread.sleep(20);
                }
            }
        }
        lastSendFuture.get();
        requireNonNull(startTime, "startTime result is none");
        requireNonNull(endTime, "endTime result is none");
        return Pair.of(startTime, endTime);
    }

    private void createMessages(String topicName)
            throws ExecutionException, InterruptedException
    {
        Future<RecordMetadata> lastSendFuture = Futures.immediateFuture(null);
        try (KafkaProducer<Long, Object> producer = testingKafka.createProducer()) {
            for (long messageNum = 0; messageNum < MESSAGE_NUM; messageNum++) {
                long key = messageNum;
                long value = messageNum;
                lastSendFuture = producer.send(new ProducerRecord<>(topicName, key, value));
            }
        }
        lastSendFuture.get();
    }
}
