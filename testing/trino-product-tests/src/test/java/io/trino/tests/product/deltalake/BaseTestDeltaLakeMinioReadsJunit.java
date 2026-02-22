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
package io.trino.tests.product.deltalake;

import io.minio.messages.Event;
import io.minio.messages.EventType;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.minio.messages.EventType.OBJECT_ACCESSED_GET;
import static io.minio.messages.EventType.OBJECT_ACCESSED_HEAD;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
abstract class BaseTestDeltaLakeMinioReadsJunit
{
    private List<Event> notifications;

    protected abstract String tableName();

    protected abstract String regionResourcePath();

    protected abstract String expectedRegionParquetObjectName();

    @BeforeEach
    void setUp(DeltaLakeMinioEnvironment env)
    {
        MinioClient client = env.createMinioClient();
        notifications = new CopyOnWriteArrayList<>();
        client.copyResourcePath(env.getBucketName(), regionResourcePath(), tableName());
        client.captureBucketNotifications(env.getBucketName(), notifications::add);
    }

    @Test
    @TestGroup.DeltaLakeMinio
    @TestGroup.ProfileSpecificTests
    void testReadRegionTable(DeltaLakeMinioEnvironment env)
    {
        try {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS delta.default.\"%s\"", tableName()));
            env.executeTrinoUpdate(format("CALL delta.system.register_table('default', '%1$s', 's3://%2$s/%1$s')", tableName(), env.getBucketName()));

            assertThat(env.executeTrino(format("SELECT count(name) FROM delta.default.\"%s\"", tableName())))
                    .containsOnly(row(5L));

            assertEventCountEventually(OBJECT_ACCESSED_HEAD, tableName() + "/_delta_log/00000000000000000000.json", 1);
            assertEventCountEventually(OBJECT_ACCESSED_GET, tableName() + "/_delta_log/00000000000000000000.json", 1);
            assertEventCountEventually(OBJECT_ACCESSED_GET, expectedRegionParquetObjectName(), 1);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS delta.default.\"%s\"", tableName()));
        }
    }

    private void assertEventCountEventually(EventType eventType, String objectName, int expectedCount)
    {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            long actual = notifications.stream()
                    .filter(event -> event.eventType() == eventType)
                    .filter(event -> normalizedObjectName(event.objectName()).equals(objectName))
                    .count();
            if (actual == expectedCount) {
                return;
            }

            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Minio notifications", e);
            }
        }

        long actual = notifications.stream()
                .filter(event -> event.eventType() == eventType)
                .filter(event -> normalizedObjectName(event.objectName()).equals(objectName))
                .count();

        assertThat(actual)
                .as("Expected %s notifications for %s", eventType, objectName)
                .isEqualTo(expectedCount);
    }

    private static String normalizedObjectName(String objectName)
    {
        return URLDecoder.decode(objectName, StandardCharsets.UTF_8);
    }
}
