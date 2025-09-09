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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
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
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExasolClientToWriteMapping
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
    public void testTypedWriteMapping()
            throws SQLException
    {
        testLongWriteMapping(TINYINT, Types.TINYINT, "tinyint", "setByte", 1L, Byte.valueOf("1"));
        testLongWriteMapping(SMALLINT, Types.SMALLINT, "smallint", "setShort", 256L, Short.valueOf("256"));
        testLongWriteMapping(INTEGER, Types.INTEGER, "integer", "setInt", 123456L, 123456);
        testLongWriteMapping(BIGINT, Types.BIGINT, "bigint", "setLong", 123456789L, 123456789L);
        testLongWriteMapping(createDecimalType(16, 6), Types.DECIMAL, "decimal(16, 6)", "setBigDecimal", 123456123456L, BigDecimal.valueOf(123456.123456));
        testInt128WriteMapping(createDecimalType(36, 12), Types.DECIMAL, "decimal(36, 12)", "setBigDecimal", Int128.valueOf("123456789012345612345678901234567890"), new BigDecimal("123456789012345612345678.901234567890"));

        testBooleanWriteMapping(Boolean.TRUE);
        testDoubleWriteMapping(1.2567);

        testSliceWriteMapping(createCharType(25), Types.NCHAR, "char(25)", "test");
        testSliceWriteMapping(createUnboundedVarcharType(), Types.VARCHAR, "varchar(20000)", "u".repeat(20000));
        testSliceWriteMapping(createVarcharType(123), Types.VARCHAR, "varchar(123)", "9".repeat(123));
    }

    private void testLongWriteMapping(Type type, int expectedJdbcType, String expectedDataType, String expectedJdbcMethodName,
                                                   Long inputValue, Object expectedJdbcValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertEquals(writeMapping.getWriteFunction().getJavaType(), long.class);
        assertEquals(writeMapping.getDataType(), expectedDataType);
        assertTrue(writeMapping.getWriteFunction() instanceof LongWriteFunction);
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(expectedJdbcType, expectedJdbcMethodName, expectedJdbcValue);
        LongWriteFunction longWriteFunction = (LongWriteFunction) writeMapping.getWriteFunction();
        longWriteFunction.set(statementMock, expectedJdbcType, inputValue);
    }

    private void testBooleanWriteMapping(boolean inputValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, BOOLEAN);
        assertEquals(writeMapping.getWriteFunction().getJavaType(), boolean.class);
        assertEquals(writeMapping.getDataType(), "boolean");
        assertTrue(writeMapping.getWriteFunction() instanceof BooleanWriteFunction);
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(Types.BOOLEAN, "setBoolean", inputValue);
        BooleanWriteFunction booleanWriteFunction = (BooleanWriteFunction) writeMapping.getWriteFunction();
        booleanWriteFunction.set(statementMock, Types.BOOLEAN, inputValue);
    }

    private void testDoubleWriteMapping(double inputValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, DOUBLE);
        assertEquals(writeMapping.getWriteFunction().getJavaType(), double.class);
        assertEquals(writeMapping.getDataType(), "double");
        assertTrue(writeMapping.getWriteFunction() instanceof DoubleWriteFunction);
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(Types.DOUBLE, "setDouble", inputValue);
        DoubleWriteFunction doubleWriteFunction = (DoubleWriteFunction) writeMapping.getWriteFunction();
        doubleWriteFunction.set(statementMock, Types.DOUBLE, inputValue);
    }

    private void testInt128WriteMapping(Type type, int expectedJdbcType, String expectedDataType, String expectedJdbcMethodName,
                                      Int128 inputValue, BigDecimal expectedJdbcValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertEquals(writeMapping.getWriteFunction().getJavaType(), Int128.class);
        assertEquals(writeMapping.getDataType(), expectedDataType);
        assertTrue(writeMapping.getWriteFunction() instanceof ObjectWriteFunction);
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(expectedJdbcType, expectedJdbcMethodName, expectedJdbcValue);
        ObjectWriteFunction objectWriteFunction = (ObjectWriteFunction) writeMapping.getWriteFunction();
        objectWriteFunction.set(statementMock, expectedJdbcType, inputValue);
    }

    private void testSliceWriteMapping(Type type, int expectedJdbcType, String expectedDataType, String inputValue)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertEquals(writeMapping.getWriteFunction().getJavaType(), Slice.class);
        assertEquals(writeMapping.getDataType(), expectedDataType);
        assertTrue(writeMapping.getWriteFunction() instanceof SliceWriteFunction);
        assertThat(writeMapping.getWriteFunction().getBindExpression()).isEqualTo("?");
        PreparedStatement statementMock = preparedStatementMock(expectedJdbcType, "setString", inputValue);
        SliceWriteFunction sliceWriteFunction = (SliceWriteFunction) writeMapping.getWriteFunction();
        sliceWriteFunction.set(statementMock, expectedJdbcType, Slices.utf8Slice(inputValue));
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
