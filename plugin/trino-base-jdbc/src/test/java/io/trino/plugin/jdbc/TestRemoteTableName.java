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

package io.trino.plugin.jdbc;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoteTableName
{
    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<RemoteTableName> codec = JsonCodec.jsonCodec(RemoteTableName.class);
        RemoteTableName table = new RemoteTableName(Optional.of("catalog"), Optional.of("schema"), "table");
        RemoteTableName roundTrip = codec.fromJson(codec.toJson(table));
        assertThat(table.getCatalogName()).isEqualTo(roundTrip.getCatalogName());
        assertThat(table.getSchemaName()).isEqualTo(roundTrip.getSchemaName());
        assertThat(table.getTableName()).isEqualTo(roundTrip.getTableName());
    }
}
