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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.testing.containers.Floci;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;

@Testcontainers
public class TestS3FileSystemFloci
        extends AbstractTestS3FileSystem
{
    protected static final String BUCKET = "test-bucket";

    @Container
    private static final Floci FLOCI = new Floci();

    @Override
    protected void initEnvironment()
    {
        FLOCI.createBucket(BUCKET);
    }

    @Override
    protected String bucket()
    {
        return BUCKET;
    }

    @Override
    protected S3Client createS3Client()
    {
        return S3Client.builder()
                .applyMutation(FLOCI::updateClient)
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY)
                        .setEndpoint(endpoint().toString())
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true)
                        .setStreamingPartSize(STREAMING_PART_SIZE),
                new S3FileSystemStats());
    }

    protected static URI endpoint()
    {
        return FLOCI.endpoint();
    }
}
