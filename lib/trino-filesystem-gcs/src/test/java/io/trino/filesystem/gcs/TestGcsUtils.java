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

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.trino.filesystem.gcs.GcsUtils.getBlob;
import static java.lang.reflect.Proxy.newProxyInstance;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGcsUtils
{
    @Test
    void testAuditHeadersPassedToGetBlob()
    {
        AtomicReference<Storage.BlobGetOption[]> options = new AtomicReference<>();
        Storage storage = (Storage) newProxyInstance(
                TestGcsUtils.class.getClassLoader(),
                new Class<?>[] {Storage.class},
                (_, method, arguments) -> {
                    assertThat(method.getName()).isEqualTo("get");
                    options.set((Storage.BlobGetOption[]) arguments[1]);
                    return null;
                });
        ImmutableMap<String, String> auditHeaders = ImmutableMap.of(
                "x-goog-custom-audit-trino-query-id", "query_id",
                "x-goog-custom-audit-trino-user", "alice");

        getBlob(storage, new GcsLocation(Location.of("gs://bucket/key")), auditHeaders);

        assertThat(options.get()).contains(Storage.BlobGetOption.extraHeaders(auditHeaders));
    }
}
