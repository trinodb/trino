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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.kms.v1.CreateCryptoKeyRequest;
import com.google.cloud.kms.v1.CreateKeyRingRequest;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.trino.testing.containers.FlociGcp;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.util.Objects.requireNonNull;

@Testcontainers
final class TestIcebergGcpEncryptionFloci
        extends BaseIcebergEncryptionFlociTest
{
    private static final String LOCATION = "global";

    @Container
    private static final FlociGcp FLOCI_GCP = new FlociGcp();

    @Override
    protected String kmsKey()
    {
        String keyRingId = "test-key-ring-" + randomNameSuffix();
        String cryptoKeyId = "test-crypto-key-" + randomNameSuffix();
        try {
            try (KeyManagementServiceClient client = KeyManagementServiceClient.create(kmsServiceSettings(FLOCI_GCP.getEndpoint()))) {
                client.createKeyRing(CreateKeyRingRequest.newBuilder()
                        .setParent(LocationName.of(FLOCI_GCP_PROJECT_ID, LOCATION).toString())
                        .setKeyRingId(keyRingId)
                        .setKeyRing(KeyRing.newBuilder().build())
                        .build());
                CryptoKey cryptoKey = client.createCryptoKey(CreateCryptoKeyRequest.newBuilder()
                        .setParent(KeyRingName.of(FLOCI_GCP_PROJECT_ID, LOCATION, keyRingId).toString())
                        .setCryptoKeyId(cryptoKeyId)
                        .setCryptoKey(CryptoKey.newBuilder()
                                .setPurpose(CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT)
                                .build())
                        .build());
                return cryptoKey.getName();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected KeyManagementClient kmsClient()
    {
        return new FlociGcpKeyManagementClient(FLOCI_GCP.getEndpoint());
    }

    private static KeyManagementServiceSettings kmsServiceSettings(URI endpoint)
            throws IOException
    {
        return KeyManagementServiceSettings.newBuilder()
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(FixedTransportChannelProvider.create(
                        GrpcTransportChannel.create(NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
                                .usePlaintext()
                                .build())))
                .build();
    }

    /**
     * A {@link KeyManagementClient} that talks to the Floci GCP Cloud KMS emulator. Iceberg's
     * {@code GcpKeyManagementClient} always targets the real Google Cloud KMS endpoint, so a
     * test-specific client is required to redirect requests to the emulator.
     */
    private static final class FlociGcpKeyManagementClient
            implements KeyManagementClient
    {
        private final URI endpoint;
        private KeyManagementServiceClient client;

        private FlociGcpKeyManagementClient(URI endpoint)
        {
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
        }

        @Override
        public void initialize(Map<String, String> properties) {}

        @Override
        public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
        {
            return client().encrypt(CryptoKeyName.parse(wrappingKeyId).toString(), ByteString.copyFrom(key.duplicate()))
                    .getCiphertext()
                    .asReadOnlyByteBuffer();
        }

        @Override
        public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
        {
            return client().decrypt(CryptoKeyName.parse(wrappingKeyId).toString(), ByteString.copyFrom(wrappedKey.duplicate()))
                    .getPlaintext()
                    .asReadOnlyByteBuffer();
        }

        private synchronized KeyManagementServiceClient client()
        {
            if (client == null) {
                try {
                    client = KeyManagementServiceClient.create(kmsServiceSettings(endpoint));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return client;
        }

        @Override
        public void close()
        {
            if (client != null) {
                client.close();
            }
        }
    }
}
