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
package io.trino.plugin.lance;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLanceTableHandle
{
    private final LanceTableHandle tableHandle = new LanceTableHandle("schemaName", "tableName", "path/to/table");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<LanceTableHandle> codec = jsonCodec(LanceTableHandle.class);
        String json = codec.toJson(tableHandle);
        LanceTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(tableHandle);
    }
}
