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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.utils.BinaryUtils;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.function.Function;

import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3FileSystemAwsS3WithSseCustomerKey
        extends AbstractTestS3FileSystem
{
    private static final String CUSTOMER_KEY = generateCustomerKey();

    private String accessKey;
    private String secretKey;
    private String region;
    private String bucket;
    private S3SseCustomerKey s3SseCustomerKey;

    @Override
    protected void initEnvironment()
    {
        accessKey = requireEnv("AWS_ACCESS_KEY_ID");
        secretKey = requireEnv("AWS_SECRET_ACCESS_KEY");
        region = requireEnv("AWS_REGION");
        bucket = requireEnv("EMPTY_S3_BUCKET");
        s3SseCustomerKey = S3SseCustomerKey.onAes256(CUSTOMER_KEY);
    }

    @Override
    protected String bucket()
    {
        return bucket;
    }

    @Override
    protected S3Client createS3Client()
    {
        S3Client s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .build();

        return new DelegatingS3Client(s3Client)
        {
            @Override
            protected <T extends S3Request, ReturnT> ReturnT invokeOperation(T request, Function<T, ReturnT> operation)
            {
                if (request instanceof PutObjectRequest putObjectRequest) {
                    PutObjectRequest.Builder putObjectRequestBuilder = putObjectRequest.toBuilder();
                    putObjectRequestBuilder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm());
                    putObjectRequestBuilder.sseCustomerKey(s3SseCustomerKey.key());
                    putObjectRequestBuilder.sseCustomerKeyMD5(s3SseCustomerKey.md5());
                    return operation.apply((T) putObjectRequestBuilder.build());
                }
                else if (request instanceof GetObjectRequest getObjectRequest) {
                    GetObjectRequest.Builder getObjectRequestBuilder = getObjectRequest.toBuilder();
                    getObjectRequestBuilder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm());
                    getObjectRequestBuilder.sseCustomerKey(s3SseCustomerKey.key());
                    getObjectRequestBuilder.sseCustomerKeyMD5(s3SseCustomerKey.md5());
                    return operation.apply((T) getObjectRequestBuilder.build());
                }
                return operation.apply(request);
            }
        };
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
                        .setAwsAccessKey(accessKey)
                        .setAwsSecretKey(secretKey)
                        .setRegion(region)
                        .setSseType(CUSTOMER)
                        .setSseCustomerKey(s3SseCustomerKey.key())
                        .setStreamingPartSize(streamingPartSize),
                new S3FileSystemStats());
    }

    private static String generateCustomerKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256, new SecureRandom());
            SecretKey secretKey = keyGenerator.generateKey();
            return BinaryUtils.toBase64(secretKey.getEncoded());
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
