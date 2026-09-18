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
package io.trino.spooling.filesystem;

import io.airlift.units.DataSize;
import io.azam.ulidj.ULID;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.QueryId;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Testcontainers
@TestInstance(PER_CLASS)
public class TestFileSystemSpoolingManagerFloci
{
    private static final String BUCKET_NAME = "test-bucket";

    @Container
    private static final Floci FLOCI = new Floci();

    @BeforeAll
    public void setup()
    {
        FLOCI.createBucket(BUCKET_NAME);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRetrieveSpooledSegment(boolean encryptionEnabled)
            throws Exception
    {
        SpoolingManager manager = createSpoolingManager(encryptionEnabled);
        SpoolingContext context = new SpoolingContext("json", QueryId.valueOf("a"), 0, 0);
        SpooledSegmentHandle spooledSegmentHandle = manager.create(context);
        try (OutputStream segment = manager.createOutputStream(spooledSegmentHandle)) {
            segment.write("data".getBytes(UTF_8));
        }

        try (InputStream output = manager.openInputStream(spooledSegmentHandle)) {
            byte[] buffer = new byte[4];
            assertThat(output.read(buffer)).isEqualTo(buffer.length);
            assertThat(buffer).isEqualTo("data".getBytes(UTF_8));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAcknowledgedSegmentCantBeRetrievedAgain(boolean encryptionEnabled)
            throws Exception
    {
        SpoolingManager manager = createSpoolingManager(encryptionEnabled);
        SpoolingContext context = new SpoolingContext("json", QueryId.valueOf("a"), 0, 0);
        SpooledSegmentHandle spooledSegmentHandle = manager.create(context);
        try (OutputStream segment = manager.createOutputStream(spooledSegmentHandle)) {
            segment.write("data".getBytes(UTF_8));
        }

        try (InputStream output = manager.openInputStream(spooledSegmentHandle)) {
            byte[] buffer = new byte[4];
            assertThat(output.read(buffer)).isEqualTo(buffer.length);
            assertThat(buffer).isEqualTo("data".getBytes(UTF_8));
        }

        manager.acknowledge(spooledSegmentHandle);
        assertThatThrownBy(() -> manager.openInputStream(spooledSegmentHandle).read())
                .isInstanceOf(IOException.class)
                .hasMessage("Segment not found or expired");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandleRoundTrip(boolean encryptionEnabled)
            throws IOException
    {
        Optional<EncryptionKey> key = Optional.empty();
        if (encryptionEnabled) {
            key = Optional.of(randomAes256());
        }
        FileSystemSpooledSegmentHandle handle = new FileSystemSpooledSegmentHandle("json", ULID.randomBinary(), "nodeId", key);
        SpooledLocation location = createSpoolingManager(encryptionEnabled).location(handle);
        FileSystemSpooledSegmentHandle handle2 = (FileSystemSpooledSegmentHandle) createSpoolingManager(encryptionEnabled).handle(location.identifier(), location.headers());

        assertThat(handle.identifier()).isEqualTo(handle2.identifier());
        assertThat(handle.uuid()).isEqualTo(handle2.uuid());
        assertThat(handle.expirationTime()).isEqualTo(handle2.expirationTime());
        assertThat(handle2.encryptionKey()).isEqualTo(key);
    }

    private SpoolingManager createSpoolingManager(boolean encryptionEnabled)
    {
        FileSystemSpoolingConfig spoolingConfig = new FileSystemSpoolingConfig();
        spoolingConfig.setS3Enabled(true);
        spoolingConfig.setLocation("s3://%s/".formatted(BUCKET_NAME));
        spoolingConfig.setEncryptionEnabled(encryptionEnabled);
        S3FileSystemConfig filesystemConfig = new S3FileSystemConfig()
                .setEndpoint(FLOCI.endpoint().toString())
                .setRegion(FLOCI_REGION)
                .setAwsAccessKey(FLOCI_ACCESS_KEY)
                .setAwsSecretKey(FLOCI_SECRET_KEY)
                .setPathStyleAccess(true)
                .setStreamingPartSize(DataSize.valueOf("5.5MB"));
        return new FileSystemSpoolingManager(spoolingConfig, new S3FileSystemFactory(noop(), filesystemConfig, new S3FileSystemStats()), new SimpleFileSystemLayout(), new TestingNode("nodeId"));
    }
}
