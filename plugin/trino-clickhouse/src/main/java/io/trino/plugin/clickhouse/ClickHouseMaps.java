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
package io.trino.plugin.clickhouse;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.MapType;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Reading ClickHouse {@code Map(String, String)} columns as {@code map(varchar, varchar)}.
 */
final class ClickHouseMaps
{
    private ClickHouseMaps() {}

    static boolean isStringMap(String clickHouseTypeName)
    {
        ClickHouseColumn column;
        try {
            column = ClickHouseColumn.of("", clickHouseTypeName);
        }
        catch (RuntimeException e) {
            return false;
        }
        return column.getDataType() == ClickHouseDataType.Map
                && isString(column.getKeyInfo())
                && isString(column.getValueInfo());
    }

    private static boolean isString(ClickHouseColumn column)
    {
        return column != null && column.getDataType() == ClickHouseDataType.String;
    }

    static ColumnMapping mapColumnMapping(MapType mapType)
    {
        return ColumnMapping.objectMapping(
                mapType,
                ObjectReadFunction.of(SqlMap.class, (resultSet, columnIndex) -> toSqlMap(mapType, resultSet.getObject(columnIndex))),
                ObjectWriteFunction.of(SqlMap.class, (_, _, _) -> {
                    throw new TrinoException(NOT_SUPPORTED, "Writing ClickHouse Map columns is not supported");
                }),
                DISABLE_PUSHDOWN);
    }

    static SqlMap toSqlMap(MapType mapType, Object value)
    {
        if (!(value instanceof Map<?, ?> map)) {
            throw new TrinoException(JDBC_ERROR, "Unexpected value for ClickHouse Map column: " + (value == null ? null : value.getClass().getName()));
        }
        return buildMapValue(mapType, map.size(), (keyBuilder, valueBuilder) -> map.forEach((key, element) -> {
            VARCHAR.writeSlice(keyBuilder, utf8Slice(String.valueOf(key)));
            if (element == null) {
                valueBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(valueBuilder, utf8Slice(element.toString()));
            }
        }));
    }

    static boolean isStringMapColumn(ColumnHandle column)
    {
        return column instanceof JdbcColumnHandle jdbcColumn
                && jdbcColumn.getJdbcTypeHandle().jdbcTypeName().filter(ClickHouseMaps::isStringMap).isPresent();
    }

    static boolean referencesStringMap(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (expression instanceof Variable variable) {
            return variable.getType() instanceof MapType || isStringMapColumn(assignments.get(variable.getName()));
        }
        return expression.getChildren().stream().anyMatch(child -> referencesStringMap(child, assignments));
    }
}
