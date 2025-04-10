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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.EquivalenceTester;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.jdbc.MetadataUtil.COLUMN_CODEC;
import static io.trino.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdbcColumnHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(
                COLUMN_CODEC,
                JdbcColumnHandle.builder()
                        .setColumnName("columnName")
                        .setJdbcTypeHandle(JDBC_VARCHAR)
                        .setColumnType(VARCHAR)
                        .setComment(Optional.of("some comment"))
                        .build());
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JdbcColumnHandle("columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnName", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("columnName", JDBC_VARCHAR, VARCHAR))
                .addEquivalentGroup(
                        new JdbcColumnHandle("columnNameX", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnNameX", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnNameX", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("columnNameX", JDBC_VARCHAR, VARCHAR))
                .check();
    }

    @Test
    public void testValidColumnPropertyType()
    {
        JdbcColumnHandle.Builder builder = generateBaseColumnHandleBuilder();
        ImmutableMap.Builder<String, Object> columnPropertiesBuilder = ImmutableMap.builder();
        columnPropertiesBuilder.put("int_property", 1);
        columnPropertiesBuilder.put("boolean_property", true);
        columnPropertiesBuilder.put("long_property", 1L);
        columnPropertiesBuilder.put("double_property", 1.0);
        columnPropertiesBuilder.put("string_property", "string");
        columnPropertiesBuilder.put("list_properties", ImmutableList.of(1, 2, 3));
        columnPropertiesBuilder.put("map_properties", ImmutableMap.of("key", "value"));
        ImmutableMap<String, Object> validColumnProperties = columnPropertiesBuilder.buildOrThrow();
        builder.setColumnProperties(validColumnProperties);
        JdbcColumnHandle jdbcColumnHandle = builder.build();

        assertThat(jdbcColumnHandle.getColumnProperties()).hasSize(7);
        assertThat(jdbcColumnHandle.getColumnProperties()).containsAllEntriesOf(validColumnProperties);
    }

    @Test
    public void testInvalidColumnPropertyType()
    {
        JdbcColumnHandle.Builder nullValueBuilder = generateBaseColumnHandleBuilder();
        Map<String, Object> nullColumnProperties = new HashMap<>();
        nullColumnProperties.put("null_property", null);
        nullValueBuilder.setColumnProperties(nullColumnProperties);
        assertThatThrownBy(() -> nullValueBuilder.build())
                .hasMessageContaining("Property value is null");

        JdbcColumnHandle.Builder mapValueBuilder = generateBaseColumnHandleBuilder();
        mapValueBuilder.setColumnProperties(ImmutableMap.of("invalid_property", Optional.empty()));
        assertThatThrownBy(() -> mapValueBuilder.build())
                .hasMessageContaining("Unsupported property value type: java.util.Optional");
    }

    private JdbcColumnHandle.Builder generateBaseColumnHandleBuilder()
    {
        JdbcColumnHandle.Builder columnBuilder = JdbcColumnHandle.builder();
        columnBuilder.setColumnName("columnName");
        columnBuilder.setColumnType(VARCHAR);
        columnBuilder.setJdbcTypeHandle(JDBC_VARCHAR);
        return columnBuilder;
    }
}
