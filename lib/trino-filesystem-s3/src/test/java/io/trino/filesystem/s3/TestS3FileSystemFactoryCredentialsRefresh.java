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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TestS3FileSystemFactoryCredentialsRefresh
{
    private static final DataSize STREAMING_PART_SIZE = DataSize.of(5, MEGABYTE);

    @Container
    private static final Floci FLOCI = new Floci();

    private final String bucket = "test-s3-credentials-refresh-" + randomNameSuffix();
    private S3FileSystemFactory fileSystemFactory;

    @BeforeAll
    void setup()
    {
        FLOCI.createBucket(bucket);
        fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY)
                        .setEndpoint(FLOCI.endpoint().toString())
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true)
                        .setStreamingPartSize(STREAMING_PART_SIZE),
                new S3FileSystemStats());
    }

    @AfterAll
    void tearDown()
    {
        fileSystemFactory.destroy();
        fileSystemFactory = null;
    }

    @Test
    void testVendedCredentialsAreResolvedDuringWriteAndClose()
            throws IOException
    {
        CountingCredentialsRefresher credentialsRefresher = new CountingCredentialsRefresher();
        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(vendedCredentials())
                .build();
        Location location = Location.of("s3://%s/%s".formatted(bucket, randomNameSuffix()));
        byte[] data = new byte[toIntExact(STREAMING_PART_SIZE.toBytes() * 3)];
        ThreadLocalRandom.current().nextBytes(data);

        TrinoFileSystem fileSystem = fileSystemFactory.create(identity, credentialsRefresher);
        OutputStream outputStream = fileSystem.newOutputFile(location).create();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(0);

        outputStream.write(data);
        outputStream.close();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(5);

        try (TrinoInputStream inputStream = fileSystem.newInputFile(location).newStream()) {
            assertThat(inputStream.readAllBytes()).isEqualTo(data);
        }
    }

    private static Map<String, String> vendedCredentials()
    {
        return ImmutableMap.of(
                EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, FLOCI_ACCESS_KEY,
                EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, FLOCI_SECRET_KEY,
                EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "session-token");
    }

    private static final class CountingCredentialsRefresher
            implements Supplier<Map<String, String>>
    {
        private final AtomicInteger refreshCount = new AtomicInteger();

        @Override
        public Map<String, String> get()
        {
            refreshCount.incrementAndGet();
            return vendedCredentials();
        }

        public int refreshCount()
        {
            return refreshCount.get();
        }
    }
}
