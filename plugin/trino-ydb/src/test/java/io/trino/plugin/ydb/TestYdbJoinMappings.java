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
package io.trino.plugin.ydb;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.JoinCondition.Operator.EQUAL;
import static io.trino.spi.connector.JoinCondition.Operator.IDENTICAL;
import static io.trino.spi.connector.JoinCondition.Operator.LESS_THAN;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbJoinMappings
{
    private final YdbClient client = new YdbClient(
            new BaseJdbcConfig(),
            _ -> {
                throw new SQLException("This test must not open a connection");
            },
            new YdbQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    @Test
    public void testSupportedNativeEqualityKeys()
    {
        for (String name : List.of(
                "Bool",
                "Int8",
                "Int16",
                "Int32",
                "Int64",
                "Uint8",
                "Uint16",
                "Uint32",
                "Uint64",
                "Float",
                "Double",
                "Utf8",
                "String",
                "Date",
                "Date32",
                "Datetime",
                "Datetime64",
                "Timestamp",
                "Timestamp64")) {
            JdbcTypeHandle handle = typeHandle(name);
            Type type = client.toColumnMapping(SESSION, null, handle).orElseThrow().getType();
            JdbcColumnHandle left = new JdbcColumnHandle("left", handle, type);
            JdbcColumnHandle right = new JdbcColumnHandle("right", handle, type);
            assertThat(client.isSupportedJoinCondition(SESSION, new JdbcJoinCondition(left, EQUAL, right))).as(name).isTrue();
            assertThat(client.isSupportedJoinCondition(SESSION, new JdbcJoinCondition(left, LESS_THAN, right))).as(name).isFalse();
            assertThat(client.isSupportedJoinCondition(SESSION, new JdbcJoinCondition(left, IDENTICAL, right))).as(name).isFalse();
        }
    }

    @Test
    public void testDecimalAndMixedTypesFallBack()
    {
        JdbcColumnHandle decimal = new JdbcColumnHandle("decimal", typeHandle("Decimal(20,0)"), createDecimalType(20, 0));
        JdbcColumnHandle unsigned = new JdbcColumnHandle("unsigned", typeHandle("Uint64"), createDecimalType(20, 0));
        JdbcColumnHandle signed = new JdbcColumnHandle("signed", typeHandle("Int64"), BIGINT);
        assertThat(client.isSupportedJoinCondition(SESSION, new JdbcJoinCondition(decimal, EQUAL, decimal))).isFalse();
        assertThat(client.isSupportedJoinCondition(SESSION, new JdbcJoinCondition(unsigned, EQUAL, decimal))).isFalse();
        assertThat(client.isSupportedJoinCondition(SESSION, new JdbcJoinCondition(unsigned, EQUAL, signed))).isFalse();
    }

    @Test
    public void testFloatingPointNormalizationAndTypedTopN()
            throws Throwable
    {
        YdbQueryBuilder queryBuilder = new YdbQueryBuilder(RemoteQueryModifier.NONE);
        for (Type type : List.of(REAL, DOUBLE)) {
            String nativeType = type.equals(REAL) ? "Float" : "Double";
            JdbcColumnHandle column = new JdbcColumnHandle("key", typeHandle(nativeType), type);
            String condition = queryBuilder.formatJoinCondition(client, "l", "r", new JdbcJoinCondition(column, EQUAL, column));
            assertThat(condition).contains("NANVL(", "CAST(NULL AS " + nativeType + ")", "l.`key`", "r.`key`");
            var values = type.createBlockBuilder(null, 2);
            if (type.equals(REAL)) {
                type.writeLong(values, Float.floatToRawIntBits(Float.NaN));
                type.writeLong(values, Float.floatToRawIntBits(1.0f));
            }
            else {
                type.writeDouble(values, Double.NaN);
                type.writeDouble(values, 1.0);
            }
            var block = values.build();
            for (SortOrder order : SortOrder.values()) {
                int comparison = (int) new TypeOperators().getOrderingOperator(
                        type,
                        order,
                        simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)).invokeWithArguments(block, 0, block, 1);
                String nanDirection = comparison < 0 ? "DESC" : "ASC";
                String topN = client.topNFunction().orElseThrow().apply(
                        "SELECT `key` FROM `test`",
                        List.of(new JdbcSortItem(column, order)),
                        2);
                assertThat(topN).contains(
                        "CASE WHEN `key` != `key` THEN 1 ELSE 0 END " + nanDirection,
                        "NANVL(`key`, CAST(0 AS " + nativeType + ")) " + (order.isAscending() ? "ASC" : "DESC"));
            }
        }
    }

    @Test
    public void testUnsupportedProjectionTypesHaveNoMapping()
    {
        assertThat(YdbTypeUtils.toTypeHandle(new ArrayType(BIGINT))).isEmpty();
        assertThat(YdbTypeUtils.toTypeHandle(createTimestampType(12))).isEmpty();
    }

    private static JdbcTypeHandle typeHandle(String name)
    {
        return new JdbcTypeHandle(Types.OTHER, Optional.of(name), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }
}
