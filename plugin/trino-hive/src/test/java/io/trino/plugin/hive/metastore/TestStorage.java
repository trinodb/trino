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
package io.trino.plugin.hive.metastore;

import io.airlift.json.JsonCodec;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStorage
{
    private static final JsonCodec<Storage> CODEC = jsonCodec(Storage.class);

    @Test
    public void testRoundTrip()
    {
        Storage storage = Storage.builder()
                .setStorageFormat(StorageFormat.create("abc", "in", "out"))
                .setLocation("/test")
                .build();

        assertThat(CODEC.fromJson(CODEC.toJson(storage))).isEqualTo(storage);
    }
}
