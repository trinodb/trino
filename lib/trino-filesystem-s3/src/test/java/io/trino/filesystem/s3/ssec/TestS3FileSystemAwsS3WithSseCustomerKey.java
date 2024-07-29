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
package io.trino.filesystem.s3.ssec;

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static java.util.Objects.requireNonNull;

public class TestS3FileSystemAwsS3WithSseCustomerKey
        extends AbstractTestS3FileSystemWithSseCustomerKey
{
    private String accessKey;
    private String secretKey;
    private String region;
    private String bucket;

    @Override
    protected void initEnvironment()
    {
        super.initEnvironment();
        accessKey = environmentVariable("AWS_ACCESS_KEY_ID");
        secretKey = environmentVariable("AWS_SECRET_ACCESS_KEY");
        region = environmentVariable("AWS_REGION");
        bucket = environmentVariable("EMPTY_S3_BUCKET");
    }

    @Override
    protected String bucket()
    {
        return bucket;
    }

    @Override
    protected S3ClientBuilder createS3ClientBuilder()
    {
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region));
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(accessKey)
                        .setAwsSecretKey(secretKey)
                        .setRegion(region)
                        .setSseType(CUSTOMER)
                        .setSseCustomerKey(s3SseCustomerKey.key())
                        .setStreamingPartSize(DataSize.valueOf("5.5MB")));
    }

    private static String environmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }
}
