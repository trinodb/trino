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

import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.type.JsonType;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static org.assertj.core.api.Assertions.assertThat;

/// Contract of the shared JSON `ColumnMapping`: because `JsonType`'s javaType is `Json`, the
/// mapping must expose object (not slice) read/write functions, and its write function must
/// render the `Json` value to text through the dialect's text-write function. A slice-typed
/// write function here is what raised the `JdbcPageSink` VerifyException. The read side of the
/// same boundary (remote text to `Json`) is driven through a real JDBC ResultSet by the
/// per-dialect `testJson` type-mapping integration tests.
public class TestJsonColumnMapping
{
    @Test
    void mappingExposesObjectFunctionsForTheJsonJavaType()
    {
        ColumnMapping mapping = StandardColumnMappings.jsonColumnMapping(JsonType.JSON, varcharWriteFunction());
        assertThat(mapping.getType().getJavaType()).isEqualTo(Json.class);
        assertThat(((ObjectWriteFunction) mapping.getWriteFunction()).getJavaType()).isEqualTo(Json.class);
        assertThat(((ObjectReadFunction) mapping.getReadFunction()).getJavaType()).isEqualTo(Json.class);
    }

    @Test
    void writeFunctionRendersJsonToTextThroughTheDialectWriter()
            throws SQLException
    {
        // Drive the write function itself, not a JsonItems round-trip: the Json value must reach the
        // dialect's text-write function as its canonical text. A recording text writer stands in for
        // the JDBC statement (which it never touches), so the conversion the mapping performs is
        // exactly what is under test -- an unwired or slice-typed write function would fail here.
        Slice[] written = new Slice[1];
        SliceWriteFunction recordingTextWriter = (_, _, value) -> written[0] = value;
        ColumnMapping mapping = StandardColumnMappings.jsonColumnMapping(JsonType.JSON, recordingTextWriter);

        Json value = JsonItems.fromText(utf8Slice("{\"a\":1,\"b\":2}"));
        ((ObjectWriteFunction) mapping.getWriteFunction()).set(null, 1, value);

        assertThat(written[0].toStringUtf8()).isEqualTo("{\"a\":1,\"b\":2}");
    }
}
