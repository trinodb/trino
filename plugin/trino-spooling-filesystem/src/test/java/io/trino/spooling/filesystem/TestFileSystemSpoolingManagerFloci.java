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
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.spool.SpoolingManager;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;

@Testcontainers
public class TestFileSystemSpoolingManagerFloci
        extends AbstractFileSystemSpoolingManagerTest
{
    private static final String BUCKET_NAME = "test-bucket";

    @Container
    private static final Floci FLOCI = new Floci();

    @BeforeAll
    public void setup()
    {
        try (S3Client s3Client = createS3Client()) {
            s3Client.createBucket(builder -> builder.bucket(BUCKET_NAME).build());
        }
    }

    @Override
    protected SpoolingManager getSpoolingManager()
    {
        FileSystemSpoolingConfig spoolingConfig = new FileSystemSpoolingConfig();
        spoolingConfig.setS3Enabled(true);
        spoolingConfig.setLocation("s3://%s/".formatted(BUCKET_NAME));
        spoolingConfig.setEncryptionEnabled(true);
        S3FileSystemConfig filesystemConfig = new S3FileSystemConfig()
                .setEndpoint(FLOCI.endpoint().toString())
                .setRegion(FLOCI_REGION)
                .setAwsAccessKey(FLOCI_ACCESS_KEY)
                .setAwsSecretKey(FLOCI_SECRET_KEY)
                .setPathStyleAccess(true)
                .setStreamingPartSize(DataSize.valueOf("5.5MB"));
        return new FileSystemSpoolingManager(spoolingConfig, new S3FileSystemFactory(noop(), filesystemConfig, new S3FileSystemStats()), new SimpleFileSystemLayout(), new TestingNode("nodeId"));
    }

    protected S3Client createS3Client()
    {
        return FLOCI.createS3Client();
    }
}
