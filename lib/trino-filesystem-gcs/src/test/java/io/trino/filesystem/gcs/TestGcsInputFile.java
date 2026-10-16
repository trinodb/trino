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
package io.trino.filesystem.gcs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpServer;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGcsInputFile
{
    @Test
    void testExistsUsesOneRequest()
            throws Exception
    {
        AtomicBoolean exists = new AtomicBoolean();
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.incrementAndGet();
                if (!exists.get()) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] metadata = "{\"bucket\":\"bucket\",\"name\":\"key\",\"generation\":\"7\",\"size\":\"10\"}".getBytes(UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, metadata.length);
                exchange.getResponseBody().write(metadata);
            }
        });
        server.start();
        try (Storage storage = StorageOptions.newBuilder()
                .setProjectId("test-project")
                .setCredentials(NoCredentials.getInstance())
                .setHost("http://localhost:" + server.getAddress().getPort())
                .build()
                .getService()) {
            GcsInputFile input = new GcsInputFile(new GcsLocation(Location.of("gs://bucket/key")), storage, 1024, OptionalLong.empty(), Optional.empty(), Optional.empty());
            assertThat(input.exists()).isFalse();
            assertThat(requests.get()).isEqualTo(1);

            exists.set(true);
            assertThat(input.exists()).isTrue();
            assertThat(requests.get()).isEqualTo(2);

            exists.set(false);
            assertThat(input.exists()).isFalse();
            assertThat(requests.get()).isEqualTo(3);
        }
        finally {
            server.stop(0);
        }
    }
}
