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
package io.trino.plugin.iceberg.encryption;

import org.apache.iceberg.aws.AwsClientFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.security.SecureRandom;
import java.util.Map;

public class TestingPropertyAwareAwsClientFactory
        implements AwsClientFactory
{
    public static final String REQUIRED_PROPERTY = "testing.aws.required-property";

    private KmsClient kmsClient;

    @Override
    public void initialize(Map<String, String> properties)
    {
        String value = properties.get(REQUIRED_PROPERTY);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: " + REQUIRED_PROPERTY);
        }
        kmsClient = new InMemoryKmsClient();
    }

    @Override
    public S3Client s3()
    {
        throw unsupportedClient("s3");
    }

    @Override
    public S3AsyncClient s3Async()
    {
        throw unsupportedClient("s3Async");
    }

    @Override
    public GlueClient glue()
    {
        throw unsupportedClient("glue");
    }

    @Override
    public KmsClient kms()
    {
        if (kmsClient == null) {
            throw new IllegalStateException("Factory must be initialized before use");
        }
        return kmsClient;
    }

    @Override
    public DynamoDbClient dynamo()
    {
        throw unsupportedClient("dynamo");
    }

    private static UnsupportedOperationException unsupportedClient(String clientName)
    {
        return new UnsupportedOperationException("Testing factory supports only kms client, not " + clientName);
    }

    private static class InMemoryKmsClient
            implements KmsClient
    {
        private final SecureRandom secureRandom = new SecureRandom();

        @Override
        public EncryptResponse encrypt(EncryptRequest request)
        {
            return EncryptResponse.builder()
                    .ciphertextBlob(request.plaintext())
                    .build();
        }

        @Override
        public DecryptResponse decrypt(DecryptRequest request)
        {
            return DecryptResponse.builder()
                    .plaintext(request.ciphertextBlob())
                    .build();
        }

        @Override
        public GenerateDataKeyResponse generateDataKey(GenerateDataKeyRequest request)
        {
            int keyLength = request.keySpec() == DataKeySpec.AES_128 ? 16 : 32;
            byte[] key = new byte[keyLength];
            secureRandom.nextBytes(key);
            SdkBytes keyBytes = SdkBytes.fromByteArray(key);

            return GenerateDataKeyResponse.builder()
                    .plaintext(keyBytes)
                    .ciphertextBlob(keyBytes)
                    .build();
        }

        @Override
        public String serviceName()
        {
            return "kms";
        }

        @Override
        public void close() {}
    }
}
