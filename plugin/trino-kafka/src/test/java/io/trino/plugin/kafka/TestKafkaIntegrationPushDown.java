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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.kafka.BasicTestingKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestKafkaIntegrationPushDown
        extends AbstractTestQueryFramework
{
    private static final int MESSAGE_NUM = 1000;
    private static final int TIMESTAMP_TEST_COUNT = 6;
    private static final int TIMESTAMP_TEST_START_INDEX = 2;
    private static final int TIMESTAMP_TEST_END_INDEX = 4;

    private BasicTestingKafka testingKafka;
    private String topicNamePartition;
    private String topicNameOffset;
    private String topicNameCreateTime;
    private String topicNameLogAppend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new BasicTestingKafka();
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
        testingKafka.createTopicWithConfig(1, 1, topicNameCreateTime, false);
        testingKafka.createTopicWithConfig(1, 1, topicNameLogAppend, true);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
            throws Exception
    {
        if (testingKafka != null) {
            testingKafka.close();
            testingKafka = null;
        }
    }

    @Test
    public void testPartitionPushDown()
            throws ExecutionException, InterruptedException
    {
        createMessages(topicNamePartition);
        String sql = format("SELECT count(*) FROM default.%s WHERE _partition_id=1", topicNamePartition);

        ResultWithQueryId<MaterializedResult> queryResult = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(getDistributedQueryRunner(), queryResult).getQueryStats().getProcessedInputPositions(), MESSAGE_NUM / 2);
    }

    @Test
    public void testOffsetPushDown()
            throws ExecutionException, InterruptedException
    {
        createMessages(topicNameOffset);
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        String sql = format("SELECT count(*) FROM default.%s WHERE _partition_offset between 2 and 10", topicNameOffset);

        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 18);

        sql = format("SELECT count(*) FROM default.%s WHERE _partition_offset > 2 and _partition_offset < 10", topicNameOffset);

        queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 14);

        sql = format("SELECT count(*) FROM default.%s WHERE _partition_offset = 3", topicNameOffset);

        queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), 2);
    }

    @Test
    public void testTimestampCreateTimeModePushDown()
            throws Exception
    {
        RecordMessage recordMessage = createTimestampTestMessages(topicNameCreateTime);
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        // ">= startTime" insure including index 2, "< endTime"  insure excluding index 4;
        String sql = format(
                "SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'",
                topicNameCreateTime,
                recordMessage.getStartTime(),
                recordMessage.getEndTime());

        // timestamp_upper_bound_force_push_down_enabled default as false.
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
        String debugDumpString = buildDebugDumpString(topicNameCreateTime, recordMessage, queryRunner, sql, queryResult);
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions())
                .describedAs("with dump:\n%s", debugDumpString)
                .isEqualTo(998);

        // timestamp_upper_bound_force_push_down_enabled set as true.
        Session sessionWithUpperBoundPushDownEnabled = Session.builder(getSession())
                .setSystemProperty("kafka.timestamp_upper_bound_force_push_down_enabled", "true")
                .build();

        queryResult = queryRunner.executeWithQueryId(sessionWithUpperBoundPushDownEnabled, sql);
        debugDumpString = buildDebugDumpString(topicNameCreateTime, recordMessage, queryRunner, sql, queryResult);
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions())
                .describedAs("with dump:\n%s", debugDumpString)
                .isEqualTo(2);
    }

    @Test
    public void testTimestampLogAppendModePushDown()
            throws Exception
    {
        RecordMessage recordMessage = createTimestampTestMessages(topicNameLogAppend);
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        // ">= startTime" insure including index 2, "< endTime"  insure excluding index 4;
        String sql = format(
                "SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'",
                topicNameLogAppend,
                recordMessage.getStartTime(),
                recordMessage.getEndTime());
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);

        String debugDumpString = buildDebugDumpString(topicNameLogAppend, recordMessage, queryRunner, sql, queryResult);
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions())
                .describedAs("with dump:\n%s", debugDumpString)
                .isEqualTo(2);
    }

    private String buildDebugDumpString(String topic,
            RecordMessage recordMessage,
            DistributedQueryRunner queryRunner,
            String sql,
            ResultWithQueryId<MaterializedResult> queryResult)
    {
        StringBuilder sb = new StringBuilder();
        // dump main messages
        sb.append("Main messages:").append("\n");
        for (int i = 0; i < recordMessage.getTestMessageSignatures().size(); i++) {
            String dumpMessage = recordMessage.getTestMessageSignatures().get(i);
            sb.append("  ").append(dumpMessage);
            if (i == TIMESTAMP_TEST_START_INDEX) {
                sb.append(", startTime:").append(recordMessage.getStartTime());
            }
            else if (i == TIMESTAMP_TEST_END_INDEX) {
                sb.append(", endTime:").append(recordMessage.getEndTime());
            }
            sb.append("\n");
        }

        // dump sql and result
        sb.append(format("test sql:%s\n", sql));
        sb.append("test sql result:").append("\n").append(buildResultsDebugDumpString(queryResult.getResult()));

        // dump data in kafka
        sql = format(
                "SELECT _partition_id,_partition_offset,_timestamp FROM default.%s WHERE _partition_offset between %s and %s order by _partition_id, _timestamp",
                topic,
                recordMessage.getStartOffset(),
                recordMessage.getStartOffset() + MESSAGE_NUM);
        MaterializedResult rows = queryRunner.execute(getSession(), sql);
        sb.append("data check result:").append("\n").append(buildResultsDebugDumpString(rows)).append("\n");
        return sb.toString();
    }

    private static String buildResultsDebugDumpString(MaterializedResult rows)
    {
        StringBuilder sb = new StringBuilder();
        for (MaterializedRow row : rows) {
            sb.append(format("  dump values:%s\n", row.toString()));
        }
        return sb.toString();
    }

    private static QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, ResultWithQueryId<MaterializedResult> queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.getQueryId());
    }

    private RecordMessage createTimestampTestMessages(String topicName)
            throws Exception
    {
        String startTime = null;
        String endTime = null;
        long startOffset = -1;
        ImmutableList.Builder<String> testMethodSignatures = ImmutableList.builder();
        Future<RecordMetadata> lastSendFuture = Futures.immediateFuture(null);
        long lastTimeStamp = -1;
        // Avoid last test case has impact on this test case when invocationCount of @Test enabled
        Thread.sleep(100);
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
                    testMethodSignatures.add(format("timestamp:%s: partitionId:%s, offset:%s", r.timestamp(), r.partition(), r.offset()));
                    if (messageNum == 0) {
                        startOffset = r.offset();
                    }
                    else if (messageNum == TIMESTAMP_TEST_START_INDEX) {
                        startTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(r.timestamp()), ZoneId.of("UTC")));
                    }
                    else if (messageNum == TIMESTAMP_TEST_END_INDEX) {
                        endTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(r.timestamp()), ZoneId.of("UTC")));
                    }
                    // Sleep for a while to ensure different timestamps for different messages..
                    Thread.sleep(100);
                }
            }
        }
        lastSendFuture.get();
        requireNonNull(startTime, "startTime result is none");
        requireNonNull(endTime, "endTime result is none");
        return new RecordMessage(startTime, endTime, startOffset, testMethodSignatures.build());
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

    private static class RecordMessage
    {
        private final String startTime;
        private final String endTime;
        private final long startOffset;
        private final List<String> testMessageSignatures;

        public RecordMessage(String startTime, String endTime, Long startOffset, List<String> testMessageSignatures)
        {
            this.startTime = startTime;
            this.endTime = endTime;
            this.startOffset = startOffset;
            this.testMessageSignatures = testMessageSignatures;
        }

        public String getStartTime()
        {
            return startTime;
        }

        public String getEndTime()
        {
            return endTime;
        }

        public Long getStartOffset()
        {
            return startOffset;
        }

        public List<String> getTestMessageSignatures()
        {
            return testMessageSignatures;
        }
    }
}
