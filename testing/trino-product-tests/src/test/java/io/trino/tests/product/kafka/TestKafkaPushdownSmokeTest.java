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
package io.trino.tests.product.kafka;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaPushdownSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String SCHEMA_NAME = "product_tests";
    private static final long NUM_MESSAGES = 100;
    private static final long TIMESTAMP_NUM_MESSAGES = 10;

    @Test
    void testPartitionPushdown(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "pushdown_partition";
        // Create topic with 2 partitions
        env.getKafka().createTopicWithConfig(2, 1, topicName, false);

        // Send messages explicitly to alternating partitions to ensure even distribution
        for (int i = 1; i <= NUM_MESSAGES; i++) {
            int partition = (int) (i % 2);
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            env.sendMessages(topicName, partition,
                    key.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                                " WHERE _partition_id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(NUM_MESSAGES / 2);
        }
    }

    @Test
    void testOffsetPushdown(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "pushdown_offset";
        // Create topic with 2 partitions
        env.getKafka().createTopicWithConfig(2, 1, topicName, false);

        // Send messages explicitly to alternating partitions to ensure even distribution
        for (int i = 1; i <= NUM_MESSAGES; i++) {
            int partition = (int) (i % 2);
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            env.sendMessages(topicName, partition,
                    key.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Test BETWEEN
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _partition_offset BETWEEN 6 AND 10")) {
                assertThat(rs.next()).isTrue();
                // 5 offsets (6,7,8,9,10) * 2 partitions = 10
                assertThat(rs.getLong(1)).isEqualTo(10);
            }

            // Test > AND <
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _partition_offset > 5 AND _partition_offset < 10")) {
                assertThat(rs.next()).isTrue();
                // 4 offsets (6,7,8,9) * 2 partitions = 8
                assertThat(rs.getLong(1)).isEqualTo(8);
            }

            // Test >= AND <=
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _partition_offset >= 5 AND _partition_offset <= 10")) {
                assertThat(rs.next()).isTrue();
                // 6 offsets (5,6,7,8,9,10) * 2 partitions = 12
                assertThat(rs.getLong(1)).isEqualTo(12);
            }

            // Test >= AND <
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _partition_offset >= 5 AND _partition_offset < 10")) {
                assertThat(rs.next()).isTrue();
                // 5 offsets (5,6,7,8,9) * 2 partitions = 10
                assertThat(rs.getLong(1)).isEqualTo(10);
            }

            // Test > AND <=
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _partition_offset > 5 AND _partition_offset <= 10")) {
                assertThat(rs.next()).isTrue();
                // 5 offsets (6,7,8,9,10) * 2 partitions = 10
                assertThat(rs.getLong(1)).isEqualTo(10);
            }

            // Test = specific offset
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _partition_offset = 5")) {
                assertThat(rs.next()).isTrue();
                // 1 offset * 2 partitions = 2
                assertThat(rs.getLong(1)).isEqualTo(2);
            }
        }
    }

    @Test
    void testCreateTimePushdown(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "pushdown_create_time";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Ensure a spread of at least TIMESTAMP_NUM_MESSAGES * 100 milliseconds
            for (int i = 1; i <= TIMESTAMP_NUM_MESSAGES; i++) {
                stmt.executeUpdate(
                        "INSERT INTO " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                                " (bigint_key, bigint_value) VALUES (" + i + ", " + i + ")");
                Thread.sleep(100);
            }

            long startKey = 4;
            long endKey = 6;

            // Get timestamps for messages with keys 4 and 6
            List<String> timestamps = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT CAST(_timestamp AS VARCHAR) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE bigint_key IN (" + startKey + ", " + endKey + ") ORDER BY bigint_key")) {
                while (rs.next()) {
                    timestamps.add(rs.getString(1));
                }
            }
            assertThat(timestamps).hasSize(2);
            String startTime = timestamps.get(0);
            String endTime = timestamps.get(1);

            // Count messages between those timestamps
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName +
                            " WHERE _timestamp >= TIMESTAMP '" + startTime + "' AND _timestamp < TIMESTAMP '" + endTime + "'")) {
                assertThat(rs.next()).isTrue();
                // Should be messages for keys 4 and 5 (endKey - startKey = 2)
                assertThat(rs.getLong(1)).isEqualTo(endKey - startKey);
            }
        }
    }
}
