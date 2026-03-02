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

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.testing.containers.MotoContainer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

import static io.trino.testing.containers.MotoContainer.MOTO_ACCESS_KEY;
import static io.trino.testing.containers.MotoContainer.MOTO_REGION;
import static io.trino.testing.containers.MotoContainer.MOTO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
public class TestS3FileSystemMoto
        extends AbstractTestS3FileSystem
{
    private static final String BUCKET = "test-bucket";

    @Container
    private static final MotoContainer MOTO = new MotoContainer();

    @Override
    protected void initEnvironment()
    {
        MOTO.createBucket(BUCKET);
    }

    @Override
    protected String bucket()
    {
        return BUCKET;
    }

    @Override
    protected S3Client createS3Client()
    {
        return S3Client.builder().applyMutation(MOTO::updateClient).build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        DataSize streamingPartSize = DataSize.valueOf("5.5MB");
        assertThat(streamingPartSize).describedAs("Configured part size should be less than test's larger file size")
                .isLessThan(LARGER_FILE_DATA_SIZE);
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(MOTO.getEndpoint().toString())
                        .setRegion(MOTO_REGION)
                        .setPathStyleAccess(true)
                        .setAwsAccessKey(MOTO_ACCESS_KEY)
                        .setAwsSecretKey(MOTO_SECRET_KEY)
                        .setStreamingPartSize(streamingPartSize),
                new S3FileSystemStats());
    }

    @Test
    @Override
    public void testPreSignedUris()
    {
        // Moto doesn't expire pre-signed URLs
        assertThatThrownBy(super::testPreSignedUris)
                .hasMessageContaining("Expecting code to raise a throwable");
    }
}
