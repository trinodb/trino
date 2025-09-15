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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConvertPredicate
{
    private static final JdbcClient JDBC_CLIENT = new ExasolClient(
            new BaseJdbcConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession
            .builder()
            .setPropertyMetadata(ImmutableList.<PropertyMetadata<?>>builder()
                    .addAll(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
                    .build())
            .build();

    private static final JdbcColumnHandle BIGINT_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_bigint")
                    .setColumnType(BIGINT)
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.BIGINT, Optional.of("int8"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                    .build();

    @Test
    public void testConvertEqual()
    {
        Optional<ParameterizedExpression> converted = JDBC_CLIENT.convertPredicate(
                SESSION,
                translateToConnectorExpression(
                        new Comparison(EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L))),
                Map.of("c_bigint_symbol", BIGINT_COLUMN));

        assertThat(converted).isPresent();
        assertThat(converted.get().expression()).isEqualTo(format("(\"c_bigint\") %s (?)", EQUAL.getValue()));
        assertThat(converted.get().parameters()).isEqualTo(List.of(new QueryParameter(BIGINT, Optional.of(42L))));
    }

    @Test
    public void testConvertOr()
    {
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new Logical(
                                        Logical.Operator.OR,
                                        List.of(
                                                new Comparison(EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L)),
                                                new Comparison(EQUAL, new Reference(BIGINT, "c_bigint_symbol_2"), new Constant(BIGINT, 415L))))),
                        Map.of(
                                "c_bigint_symbol", BIGINT_COLUMN,
                                "c_bigint_symbol_2", BIGINT_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("((\"c_bigint\") = (?)) OR ((\"c_bigint\") = (?))");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(BIGINT, Optional.of(42L)),
                new QueryParameter(BIGINT, Optional.of(415L))));
    }

    @Test
    public void testConvertOrWithAnd()
    {
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new Logical(
                                        Logical.Operator.OR,
                                        List.of(
                                                new Comparison(EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L)),
                                                new Logical(
                                                        Logical.Operator.AND,
                                                        List.of(
                                                                new Comparison(EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 43L)),
                                                                new Comparison(EQUAL, new Reference(BIGINT, "c_bigint_symbol_2"), new Constant(BIGINT, 44L))))))),
                        Map.of(
                                "c_bigint_symbol", BIGINT_COLUMN,
                                "c_bigint_symbol_2", BIGINT_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("((\"c_bigint\") = (?)) OR (((\"c_bigint\") = (?)) AND ((\"c_bigint\") = (?)))");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(BIGINT, Optional.of(42L)),
                new QueryParameter(BIGINT, Optional.of(43L)),
                new QueryParameter(BIGINT, Optional.of(44L))));
    }

    private ConnectorExpression translateToConnectorExpression(Expression expression)
    {
        return ConnectorExpressionTranslator.translate(TEST_SESSION, expression)
                .orElseThrow();
    }
}
