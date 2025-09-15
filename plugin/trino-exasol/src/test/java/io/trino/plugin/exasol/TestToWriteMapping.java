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
package io.trino.plugin.exasol;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestToWriteMapping
{
    private static final JdbcClient CLIENT = new ExasolClient(
            new BaseJdbcConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;

    @Test
    void testShortDecimalMapping()
            throws SQLException
    {
        testShortDecimalMapping(createDecimalType(16, 6), Types.DECIMAL, "decimal(16, 6)",
                123456123456L, BigDecimal.valueOf(123456.123456));
        testShortDecimalMapping(createDecimalType(18, 6), Types.DECIMAL, "decimal(18, 6)",
                12345612345678L, BigDecimal.valueOf(12345612.345678));
        testShortDecimalMapping(createDecimalType(18, 0), Types.DECIMAL, "decimal(18, 0)",
                1L, BigDecimal.valueOf(1));
        testShortDecimalMapping(createDecimalType(18, 0), Types.DECIMAL, "decimal(18, 0)",
                123456789012345678L, BigDecimal.valueOf(123456789012345678L));
    }

    @Test
    void testLongDecimalMapping()
            throws SQLException
    {
        testLongDecimalMapping(createDecimalType(36, 12), Types.DECIMAL, "decimal(36, 12)",
                Int128.valueOf("123456789012345612345678901234567890"), new BigDecimal("123456789012345612345678.901234567890"));
        testLongDecimalMapping(createDecimalType(19, 0), Types.DECIMAL, "decimal(19, 0)",
                Int128.valueOf(1), BigDecimal.valueOf(1));
        testLongDecimalMapping(createDecimalType(19, 0), Types.DECIMAL, "decimal(19, 0)",
                Int128.valueOf("1234567890123456789"), BigDecimal.valueOf(1234567890123456789L));
    }

    private void testShortDecimalMapping(Type type, int expectedJdbcType, String expectedDataType,
                                         Long inputValue, Object expectedJdbcValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertThat(writeMapping.getWriteFunction().getJavaType()).isEqualTo(long.class);
        assertThat(writeMapping.getDataType()).isEqualTo(expectedDataType);
        assertThat(writeMapping.getWriteFunction() instanceof LongWriteFunction).isTrue();
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(expectedJdbcType, "setBigDecimal", expectedJdbcValue);
        LongWriteFunction longWriteFunction = (LongWriteFunction) writeMapping.getWriteFunction();
        longWriteFunction.set(statementMock, expectedJdbcType, inputValue);
    }

    private void testLongDecimalMapping(Type type, int expectedJdbcType, String expectedDataType, Int128 inputValue, BigDecimal expectedJdbcValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertThat(writeMapping.getWriteFunction().getJavaType()).isEqualTo(Int128.class);
        assertThat(writeMapping.getDataType()).isEqualTo(expectedDataType);
        assertThat(writeMapping.getWriteFunction() instanceof ObjectWriteFunction).isTrue();
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(expectedJdbcType, "setBigDecimal", expectedJdbcValue);
        ObjectWriteFunction objectWriteFunction = (ObjectWriteFunction) writeMapping.getWriteFunction();
        objectWriteFunction.set(statementMock, expectedJdbcType, inputValue);
    }

    private PreparedStatement preparedStatementMock(int expectedJdbcType, String expectedMethodName, Object expectedValue)
    {
        return newProxy(PreparedStatement.class, (proxy, method, args) ->
        {
            assertThat(method.getName()).isEqualTo(expectedMethodName);
            assertThat(args.length).isEqualTo(2);
            assertThat(args[0]).isEqualTo(expectedJdbcType);
            assertThat(args[1])
                    .describedAs("expected jdbc value")
                    .isEqualTo(expectedValue);

            return null;
        });
    }
}
