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
package io.trino.plugin.exchange.filesystem.s3;

import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.exchange.filesystem.CommitManifest;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config.S3SseType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config.S3SseType.KMS;
import static io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.CompatibilityMode.AWS;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.services.s3.model.StorageClass.STANDARD_IA;

class TestS3CommitManifest
{
    @Test
    void testManifestRoundTripAndUploadSettings()
            throws IOException
    {
        for (S3SseType sseType : S3SseType.values()) {
            assertManifestRoundTripAndUploadSettings(sseType);
        }
    }

    private static void assertManifestRoundTripAndUploadSettings(S3SseType sseType)
            throws IOException
    {
        var body = new AtomicReference<byte[]>();
        var headers = new AtomicReference<Headers>();
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.ofLiteral("127.0.0.1"), 0), 0);
        server.createContext("/bucket/exchange/committed", request -> {
            try {
                switch (request.getRequestMethod()) {
                    case "PUT" -> {
                        headers.set(request.getRequestHeaders());
                        body.set(request.getRequestBody().readAllBytes());
                        request.sendResponseHeaders(200, -1);
                    }
                    case "GET" -> {
                        byte[] data = body.get();
                        request.sendResponseHeaders(200, data.length);
                        request.getResponseBody().write(data);
                    }
                    default -> request.sendResponseHeaders(400, -1);
                }
            }
            finally {
                request.close();
            }
        });
        server.start();
        try {
            var config = new ExchangeS3Config()
                    .setS3AwsAccessKey("test-access-key")
                    .setS3AwsSecretKey("test-secret-key")
                    .setS3Region("us-east-1")
                    .setS3Endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                    .setS3PathStyleAccess(true)
                    .setStorageClass(STANDARD_IA)
                    .setSseType(sseType);
            if (sseType == KMS) {
                config.setSseKmsKeyId("test-kms-key");
            }
            try (var storage = new S3FileSystemExchangeStorage(new S3FileSystemExchangeStorageStats(), config, AWS)) {
                URI directory = URI.create("s3://bucket/exchange/");
                var manifest = new CommitManifest(ImmutableList.of(new FileStatus("s3://bucket/exchange/0_0.data", 12345)));
                getFutureValue(storage.writeMarkerFile(directory, manifest));
                assertThat(getFutureValue(storage.readMarkerFile(directory)).files()).containsExactlyElementsOf(manifest.files());
                assertThat(getFutureValue(storage.readMarkerFile(directory.resolve(CommitManifest.FILE_NAME))).files()).containsExactlyElementsOf(manifest.files());
                assertThat(headers.get().getFirst("x-amz-storage-class")).isEqualTo(STANDARD_IA.toString());
                switch (sseType) {
                    case NONE -> assertThat(headers.get().getFirst("x-amz-server-side-encryption")).isNull();
                    case S3 -> assertThat(headers.get().getFirst("x-amz-server-side-encryption")).isEqualTo("AES256");
                    case KMS -> {
                        assertThat(headers.get().getFirst("x-amz-server-side-encryption")).isEqualTo("aws:kms");
                        assertThat(headers.get().getFirst("x-amz-server-side-encryption-aws-kms-key-id")).isEqualTo("test-kms-key");
                    }
                }
            }
        }
        finally {
            server.stop(0);
        }
    }
}
