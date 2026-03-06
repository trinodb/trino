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
package io.trino.plugin.tibero;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTiberoClient
{
    private static final JdbcClient CLIENT = new TiberoClient(
            new BaseJdbcConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;

    @Test
    public void testTypedNullWriteMapping()
            throws SQLException
    {
        testTypedNullWriteMapping(SMALLINT, "smallint", Types.VARCHAR);
        testTypedNullWriteMapping(INTEGER, "integer", Types.VARCHAR);
        testTypedNullWriteMapping(BIGINT, "bigint", Types.VARCHAR);
        testTypedNullWriteMapping(REAL, "real", Types.VARCHAR);
        testTypedNullWriteMapping(DOUBLE, "double precision", Types.VARCHAR);
        testTypedNullWriteMapping(createCharType(25), "char(25)", Types.VARCHAR);
        testTypedNullWriteMapping(createUnboundedVarcharType(), "varchar", Types.VARCHAR);
        testTypedNullWriteMapping(createVarcharType(123), "varchar(123)", Types.VARCHAR);
    }

    @Test
    public void testTimestampWriteMapping()
            throws SQLException
    {
        testTimestampWriteMapping(TIMESTAMP_SECONDS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS')", Types.VARCHAR);
        testTimestampWriteMapping(TIMESTAMP_MILLIS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF3')", Types.VARCHAR);
        testTimestampWriteMapping(TIMESTAMP_MICROS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF6')", Types.VARCHAR);
        testTimestampWriteMapping(TIMESTAMP_NANOS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF9')", Types.VARCHAR);
    }

    private void testTypedNullWriteMapping(Type type, String dataType, int nullJdbcType)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertThat(writeMapping.getWriteFunction()).isNotNull();
        assertThat(writeMapping.getDataType()).isEqualTo(dataType);

        WriteFunction writeFunction = writeMapping.getWriteFunction();
        PreparedStatement statement = newProxy(PreparedStatement.class, (proxy, method, args) -> {
            if (method.getName().equals("setNull")) {
                assertThat(args[1]).isEqualTo(nullJdbcType);
                return null;
            }
            throw new UnsupportedOperationException("Unexpected method call: " + method.getName());
        });

        writeFunction.setNull(statement, 1);
    }

    private void testTimestampWriteMapping(Type type, String bindExpression, int nullJdbcType)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertThat(writeMapping.getWriteFunction()).isNotNull();
        WriteFunction writeFunction = writeMapping.getWriteFunction();

        assertThat(writeFunction.getBindExpression()).isEqualTo(bindExpression);

        PreparedStatement statement = newProxy(PreparedStatement.class, (proxy, method, args) -> {
            if (method.getName().equals("setNull")) {
                assertThat(args[1]).isEqualTo(nullJdbcType);
                return null;
            }
            throw new UnsupportedOperationException("Unexpected method call: " + method.getName());
        });

        writeFunction.setNull(statement, 1);
    }
}
