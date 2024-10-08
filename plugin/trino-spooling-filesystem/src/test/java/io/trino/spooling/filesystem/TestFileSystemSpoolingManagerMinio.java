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
import io.trino.spi.protocol.SpoolingManager;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.testing.containers.Minio.MINIO_REGION;

public class TestFileSystemSpoolingManagerMinio
        extends AbstractFileSystemSpoolingManagerTest
{
    private static final String BUCKET_NAME = "spooling" + UUID.randomUUID().toString()
            .replace("-", "");

    private Minio minio;

    @BeforeAll
    public void setup()
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(BUCKET_NAME);
    }

    @AfterAll
    public void teardown()
    {
        minio.stop();
    }

    @Override
    protected SpoolingManager getSpoolingManager()
    {
        FileSystemSpoolingConfig spoolingConfig = new FileSystemSpoolingConfig();
        spoolingConfig.setS3Enabled(true);
        spoolingConfig.setLocation("s3://%s/".formatted(BUCKET_NAME));
        spoolingConfig.setEncryptionEnabled(false); // Minio doesn't support SSE-C without TLS
        S3FileSystemConfig filesystemConfig = new S3FileSystemConfig()
                .setEndpoint(minio.getMinioAddress())
                .setRegion(MINIO_REGION)
                .setPathStyleAccess(true)
                .setAwsAccessKey(Minio.MINIO_ACCESS_KEY)
                .setAwsSecretKey(Minio.MINIO_SECRET_KEY)
                .setStreamingPartSize(DataSize.valueOf("5.5MB"));
        return new FileSystemSpoolingManager(spoolingConfig, new S3FileSystemFactory(noop(), filesystemConfig, new S3FileSystemStats()));
    }
}
