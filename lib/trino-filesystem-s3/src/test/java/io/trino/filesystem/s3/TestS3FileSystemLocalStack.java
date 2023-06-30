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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Testcontainers
public class TestS3FileSystemLocalStack
        extends AbstractTestS3FileSystem
{
    private static final String BUCKET = "test-bucket";

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
            .withServices(Service.S3);

    @Override
    protected void initEnvironment()
    {
        try (S3Client s3Client = createS3Client()) {
            s3Client.createBucket(builder -> builder.bucket(BUCKET).build());
        }
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
                .endpointOverride(LOCALSTACK.getEndpointOverride(Service.S3))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(new S3FileSystemConfig()
                .setAwsAccessKey(LOCALSTACK.getAccessKey())
                .setAwsSecretKey(LOCALSTACK.getSecretKey())
                .setEndpoint(LOCALSTACK.getEndpointOverride(Service.S3).toString())
                .setRegion(LOCALSTACK.getRegion())
                .setStreamingPartSize(DataSize.valueOf("5.5MB")));
    }
}
