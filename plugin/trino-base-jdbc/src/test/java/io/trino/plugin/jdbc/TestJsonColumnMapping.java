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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.JsonType;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

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
    void mappingWorksThroughPluginClassLoader()
            throws Exception
    {
        List<URL> classPath = Stream.of(
                        StandardColumnMappings.class,
                        Json.class,
                        ImmutableList.class,
                        DateTimeZone.class,
                        ObjectMapper.class,
                        ObjectMapper.class.getSuperclass())
                .map(type -> type.getProtectionDomain().getCodeSource().getLocation())
                .toList();
        try (PluginClassLoader loader = PluginManager.createClassLoader("json-mapping-test", classPath)) {
            Class<?> mappings = loader.loadClass(StandardColumnMappings.class.getName());
            Class<?> textWriterType = loader.loadClass(SliceWriteFunction.class.getName());
            Slice[] written = new Slice[1];
            Object textWriter = Proxy.newProxyInstance(loader, new Class<?>[] {textWriterType}, (_, method, arguments) -> switch (method.getName()) {
                case "getJavaType" -> Slice.class;
                case "getBindExpression" -> "?";
                case "set" -> {
                    written[0] = (Slice) arguments[2];
                    yield null;
                }
                default -> throw new UnsupportedOperationException(method.getName());
            });
            Object mapping = mappings.getMethod("jsonColumnMapping", Type.class, textWriterType)
                    .invoke(null, JsonType.JSON, textWriter);

            assertThat(mappings.getClassLoader()).isSameAs(loader);
            assertThat(loader.loadClass(Json.class.getName())).isSameAs(JsonType.JSON.getJavaType());
            assertThat(loader.loadClass(ObjectMapper.class.getName()).getClassLoader()).isSameAs(loader);

            String text = "{\"a\":1,\"a\":2}";
            ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class}, (_, method, _) -> {
                if (method.getName().equals("getString")) {
                    return text;
                }
                throw new UnsupportedOperationException(method.getName());
            });
            Object reader = mapping.getClass().getMethod("getReadFunction").invoke(mapping);
            Object value = loader.loadClass(ObjectReadFunction.class.getName())
                    .getMethod("readObject", ResultSet.class, int.class)
                    .invoke(reader, resultSet, 1);
            assertThat(value).isInstanceOf(Json.class);
            assertThat(JsonItems.toText((Json) value)).isEqualTo(utf8Slice(text));

            BlockBuilder block = JsonType.JSON.createBlockBuilder(null, 1);
            JsonType.JSON.writeObject(block, value);
            Object writer = mapping.getClass().getMethod("getWriteFunction").invoke(mapping);
            loader.loadClass(ObjectWriteFunction.class.getName())
                    .getMethod("set", PreparedStatement.class, int.class, Object.class)
                    .invoke(writer, null, 1, JsonType.JSON.getObject(block.build(), 0));
            assertThat(written[0]).isEqualTo(utf8Slice(text));
        }
    }

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
