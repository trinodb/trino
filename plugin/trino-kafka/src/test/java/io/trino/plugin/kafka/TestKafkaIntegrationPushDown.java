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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestKafkaIntegrationPushDown
        extends AbstractTestQueryFramework
{
    private static final int MESSAGE_NUM = 1000;
    private static final int TIMESTAMP_TEST_COUNT = 6;
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
        testingKafka = closeAfterClass(TestingKafka.create());
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
                        .buildOrThrow())
                .setExtraKafkaProperties(ImmutableMap.of("kafka.messages-per-split", "100"))
                .build();
        testingKafka.createTopicWithConfig(2, 1, topicNamePartition, false);
        testingKafka.createTopicWithConfig(2, 1, topicNameOffset, false);
        testingKafka.createTopicWithConfig(1, 1, topicNameCreateTime, false);
        testingKafka.createTopicWithConfig(1, 1, topicNameLogAppend, true);
        return queryRunner;
    }

    @Test
    public void testPartitionPushDown()
    {
        createMessages(topicNamePartition);
        String sql = format("SELECT count(*) FROM default.%s WHERE _partition_id=1", topicNamePartition);

        assertEventually(() -> {
            MaterializedResultWithQueryId queryResult = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
            assertEquals(getQueryInfo(getDistributedQueryRunner(), queryResult).getQueryStats().getProcessedInputPositions(), MESSAGE_NUM / 2);
        });
    }

    @Test
    public void testOffsetPushDown()
    {
        createMessages(topicNameOffset);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _partition_offset between 2 and 10", topicNameOffset), 18);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _partition_offset > 2 and _partition_offset < 10", topicNameOffset), 14);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _partition_offset = 3", topicNameOffset), 2);
    }

    @Test
    public void testTimestampCreateTimeModePushDown()
            throws Exception
    {
        RecordMessage recordMessage = createTimestampTestMessages(topicNameCreateTime);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp < timestamp '%s'", topicNameCreateTime, recordMessage.getEndTime()), 1000);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp <= timestamp '%s'", topicNameCreateTime, recordMessage.getEndTime()), 1000);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp > timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime()), 997);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime()), 998);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp between timestamp '%s' and timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime(), recordMessage.getEndTime()), 998);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime(), recordMessage.getEndTime()), 998);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp = timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime()), 998);

        // timestamp_upper_bound_force_push_down_enabled set as true.
        Session sessionWithUpperBoundPushDownEnabled = Session.builder(getSession())
                .setSystemProperty("kafka.timestamp_upper_bound_force_push_down_enabled", "true")
                .build();
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp < timestamp '%s'", topicNameCreateTime, recordMessage.getEndTime()), 4, sessionWithUpperBoundPushDownEnabled);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp <= timestamp '%s'", topicNameCreateTime, recordMessage.getEndTime()), 4, sessionWithUpperBoundPushDownEnabled);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp > timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime()), 997, sessionWithUpperBoundPushDownEnabled);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime()), 998, sessionWithUpperBoundPushDownEnabled);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp between timestamp '%s' and timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime(), recordMessage.getEndTime()), 2, sessionWithUpperBoundPushDownEnabled);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime(), recordMessage.getEndTime()), 2, sessionWithUpperBoundPushDownEnabled);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp = timestamp '%s'", topicNameCreateTime, recordMessage.getStartTime()), 0, sessionWithUpperBoundPushDownEnabled);
    }

    @Test
    public void testTimestampLogAppendModePushDown()
            throws Exception
    {
        RecordMessage recordMessage = createTimestampTestMessages(topicNameLogAppend);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp < timestamp '%s'", topicNameLogAppend, recordMessage.getEndTime()), 4);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp <= timestamp '%s'", topicNameLogAppend, recordMessage.getEndTime()), 4);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp > timestamp '%s'", topicNameLogAppend, recordMessage.getStartTime()), 997);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s'", topicNameLogAppend, recordMessage.getStartTime()), 998);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp between timestamp '%s' and timestamp '%s'", topicNameLogAppend, recordMessage.getStartTime(), recordMessage.getEndTime()), 2);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'", topicNameLogAppend, recordMessage.getStartTime(), recordMessage.getEndTime()), 2);
        assertProcessedInputPositions(format("SELECT count(*) FROM default.%s WHERE _timestamp = timestamp '%s'", topicNameLogAppend, recordMessage.getStartTime()), 0);
    }

    private void assertProcessedInputPositions(String sql, long expectedProcessedInputPositions)
    {
        assertProcessedInputPositions(sql, expectedProcessedInputPositions, getSession());
    }

    private void assertProcessedInputPositions(String sql, long expectedProcessedInputPositions, Session session)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        assertEventually(() -> {
            MaterializedResultWithQueryId queryResult = queryRunner.executeWithQueryId(session, sql);
            assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), expectedProcessedInputPositions);
        });
    }

    private static QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, MaterializedResultWithQueryId queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.getQueryId());
    }

    private RecordMessage createTimestampTestMessages(String topicName)
            throws Exception
    {
        String startTime = null;
        String endTime = null;
        for (int messageNum = 0; messageNum < TIMESTAMP_TEST_COUNT; messageNum++) {
            long key = messageNum;
            long value = messageNum;
            RecordMetadata recordMetadata = testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topicName, key, value)));
            if (messageNum == TIMESTAMP_TEST_START_INDEX) {
                startTime = getTimestamp(recordMetadata);
            }
            else if (messageNum == TIMESTAMP_TEST_END_INDEX) {
                endTime = getTimestamp(recordMetadata);
            }

            // Sleep for a while to ensure different timestamps for different messages..
            Thread.sleep(100);
        }
        testingKafka.sendMessages(
                LongStream.range(TIMESTAMP_TEST_COUNT, MESSAGE_NUM)
                        .mapToObj(id -> new ProducerRecord<>(topicName, id, id)));
        return new RecordMessage(startTime, endTime);
    }

    private static String getTimestamp(RecordMetadata recordMetadata)
    {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(recordMetadata.timestamp()), ZoneId.of("UTC")));
    }

    private void createMessages(String topicName)
    {
        testingKafka.sendMessages(
                IntStream.range(0, MESSAGE_NUM)
                        .mapToObj(id -> new ProducerRecord<>(topicName, (long) id, (long) id)));
    }

    private static class RecordMessage
    {
        private final String startTime;
        private final String endTime;

        public RecordMessage(String startTime, String endTime)
        {
            this.startTime = requireNonNull(startTime, "startTime result is none");
            this.endTime = requireNonNull(endTime, "endTime result is none");
        }

        public String getStartTime()
        {
            return startTime;
        }

        public String getEndTime()
        {
            return endTime;
        }
    }
}
