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
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

class TestYdbClient
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
    void testNullIfParameterOrder()
    {
        Call expression = new Call(BIGINT, NULLIF_FUNCTION_NAME, List.of(new Constant(11L, BIGINT), new Variable("v", BIGINT)));
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        var result = client.convertPredicate(SESSION, expression, Map.of("v", column)).orElseThrow();
        assertThat(result.parameters()).extracting(parameter -> parameter.getValue().orElseThrow()).containsExactly(11L, 11L);
    }

    @Test
    void testStringPositionParameterOrder()
    {
        Call expression = new Call(BIGINT, new FunctionName("strpos"), List.of(
                new Constant(utf8Slice("hello"), VARCHAR), new Variable("v", VARCHAR)));
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(VARCHAR).orElseThrow(), VARCHAR);
        JdbcTableHandle table = new JdbcTableHandle(new SchemaTableName("default", "test"), new RemoteTableName(Optional.empty(), Optional.empty(), "test"), Optional.empty());
        var result = client.convertProjection(SESSION, table, expression, Map.of("v", column)).orElseThrow();
        assertThat(result.getParameters()).extracting(parameter -> parameter.getValue().orElseThrow()).containsExactly(utf8Slice("hello"), utf8Slice("hello"));
    }

    @Test
    void testVirtualSchemaIsNotPassedToMetadata()
            throws Exception
    {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaData.class.getClassLoader(),
                new Class<?>[] {DatabaseMetaData.class},
                (_, method, args) -> {
                    if (method.getName().equals("getSearchStringEscape")) {
                        return "\\";
                    }
                    assertThat(method.getName()).isEqualTo("getColumns");
                    assertThat(args).containsExactly(null, null, "directory/table_with_underscores", null);
                    return null;
                });
        client.getColumns(new RemoteTableName(Optional.empty(), Optional.of("default"), "directory/table_with_underscores"), metadata);
    }

    @Test
    void testOverflowingArithmeticIsNotPushedDown()
    {
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        assertThat(client.convertPredicate(SESSION, new Call(BIGINT, ADD_FUNCTION_NAME, List.of(
                new Variable("v", BIGINT), new Constant(1L, BIGINT))), Map.of("v", column))).isEmpty();
        assertThat(client.convertPredicate(SESSION, new Call(BIGINT, DIVIDE_FUNCTION_NAME, List.of(
                new Variable("v", BIGINT), new Constant(-1L, BIGINT))), Map.of("v", column))).isEmpty();
    }
}
