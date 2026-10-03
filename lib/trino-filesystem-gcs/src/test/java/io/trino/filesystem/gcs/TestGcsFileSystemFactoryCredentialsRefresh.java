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
package io.trino.filesystem.gcs;

import com.google.cloud.NoCredentials;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.FlociGcp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.lang.Math.toIntExact;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TestGcsFileSystemFactoryCredentialsRefresh
{
    private static final DataSize WRITE_BLOCK_SIZE = DataSize.of(256, KILOBYTE);

    @Container
    private static final FlociGcp FLOCI_GCP = new FlociGcp();

    private final String bucket = "test-gcs-credentials-refresh-" + randomNameSuffix();
    private GcsFileSystemFactory fileSystemFactory;

    @BeforeAll
    void setup()
    {
        FLOCI_GCP.createBucket(bucket);
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setEndpoint(Optional.of(FLOCI_GCP.getEndpoint().toString()))
                .setProjectId(FLOCI_GCP_PROJECT_ID)
                .setWriteBlockSize(WRITE_BLOCK_SIZE);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, (builder, _) -> builder.setCredentials(NoCredentials.getInstance()));
        fileSystemFactory = new GcsFileSystemFactory(config, storageFactory);
    }

    @AfterAll
    void tearDown()
    {
        fileSystemFactory.stop();
        fileSystemFactory = null;
    }

    @Test
    void testExpiringVendedTokenIsRefreshedDuringFsOperation()
            throws IOException
    {
        CountingCredentialsRefresher credentialsRefresher = new CountingCredentialsRefresher();
        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(expiringVendedCredentials())
                .build();
        Location location = Location.of("gs://%s/%s".formatted(bucket, randomNameSuffix()));
        byte[] data = new byte[toIntExact(WRITE_BLOCK_SIZE.toBytes() * 3)];
        ThreadLocalRandom.current().nextBytes(data);

        TrinoFileSystem fileSystem = fileSystemFactory.create(identity, credentialsRefresher);
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(0);

        OutputStream outputStream = fileSystem.newOutputFile(location).create();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(1);

        outputStream.write(data);
        // the token provided by credentialsRefresher is always in refresh margin so write of each chunk (block) actually triggers refresh
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(4);

        outputStream.close();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(5);

        try (TrinoInputStream inputStream = fileSystem.newInputFile(location).newStream()) {
            assertThat(inputStream.readAllBytes()).isEqualTo(data);
        }
    }

    @Test
    void testNotExpiringVendedTokenIsNotRefreshedDuringFsOperation()
            throws IOException
    {
        CountingCredentialsRefresher credentialsRefresher = new CountingCredentialsRefresher();
        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(notExpiringVendedCredentials())
                .build();
        Location location = Location.of("gs://%s/%s".formatted(bucket, randomNameSuffix()));
        byte[] data = new byte[toIntExact(WRITE_BLOCK_SIZE.toBytes() * 3)];
        ThreadLocalRandom.current().nextBytes(data);

        TrinoFileSystem fileSystem = fileSystemFactory.create(identity, credentialsRefresher);
        OutputStream outputStream = fileSystem.newOutputFile(location).create();
        outputStream.write(data);
        outputStream.close();

        assertThat(credentialsRefresher.refreshCount()).isEqualTo(0);
        try (TrinoInputStream inputStream = fileSystem.newInputFile(location).newStream()) {
            assertThat(inputStream.readAllBytes()).isEqualTo(data);
        }
    }

    private static Map<String, String> expiringVendedCredentials()
    {
        return vendedCredentials(Instant.now().plus(1, MINUTES));
    }

    private static Map<String, String> notExpiringVendedCredentials()
    {
        return vendedCredentials(Instant.now().plus(1, HOURS));
    }

    private static Map<String, String> vendedCredentials(Instant expiresAt)
    {
        return ImmutableMap.of(
                EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "dummy",
                EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY, Long.toString(expiresAt.toEpochMilli()));
    }

    private static final class CountingCredentialsRefresher
            implements Supplier<Map<String, String>>
    {
        private final AtomicInteger refreshCount = new AtomicInteger();

        @Override
        public Map<String, String> get()
        {
            refreshCount.incrementAndGet();
            return expiringVendedCredentials();
        }

        public int refreshCount()
        {
            return refreshCount.get();
        }
    }
}
