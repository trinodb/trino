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
package io.trino.plugin.pinot;

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.pinot.TestPinotQueryBase.testingPinotTableHandle;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotTableHandle
{
    private final PinotTableHandle tableHandle = testingPinotTableHandle("schemaName", "tableName", Optional.empty());

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<PinotTableHandle> codec = jsonCodec(PinotTableHandle.class);
        String json = codec.toJson(tableHandle);
        PinotTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        testingPinotTableHandle("schema", "table", Optional.empty()),
                        testingPinotTableHandle("schema", "table", Optional.empty()))
                .addEquivalentGroup(
                        testingPinotTableHandle("schemaX", "table", Optional.empty()),
                        testingPinotTableHandle("schemaX", "table", Optional.empty()))
                .addEquivalentGroup(
                        testingPinotTableHandle("schema", "tableX", Optional.empty()),
                        testingPinotTableHandle("schema", "tableX", Optional.empty()))
                .check();
    }

    public static PinotTableHandle newTableHandle(String schemaName, String tableName)
    {
        return new PinotTableHandle(
                schemaName,
                tableName,
                false,
                TupleDomain.all(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                Optional.empty());
    }
}
