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
package io.trino.plugin.trino;

import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTrinoDelegationAnalyzer
{
    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(
            Types.BIGINT,
            Optional.of("bigint"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final TrinoDelegationAnalyzer analyzer = new TrinoDelegationAnalyzer(
            new TrinoRemoteSqlRenderer(name -> "\"" + name + "\"", new TrinoCompatibilityRegistry()));
    private final TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.forTesting(Set.of());

    @Test
    void testRemoteDelegate()
    {
        assertThat(analyzer.analyzePredicate(
                session(true),
                new Call(
                        BOOLEAN,
                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(new Variable("nationkey", BIGINT), new Variable("regionkey", BIGINT))),
                assignments(),
                capabilities))
                .isPresent();
    }

    @Test
    void testFallbackForUnsupportedFunction()
    {
        assertThat(analyzer.analyzePredicate(
                session(true),
                new Call(BOOLEAN, new FunctionName("remote_only_custom_function"), List.of(new Variable("nationkey", BIGINT))),
                assignments(),
                capabilities))
                .isEmpty();
    }

    @Test
    void testDisabledDelegationRendersNothing()
    {
        assertThat(analyzer.analyzePredicate(
                session(false),
                new Call(
                        BOOLEAN,
                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(new Variable("nationkey", BIGINT), new Variable("regionkey", BIGINT))),
                assignments(),
                capabilities))
                .isEmpty();
    }

    private static ConnectorSession session(boolean delegationEnabled)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new TrinoRemoteDelegationSessionProperties(new TrinoRemoteDelegationConfig()).getSessionProperties())
                .setPropertyValues(Map.of(TrinoRemoteDelegationSessionProperties.REMOTE_DELEGATION_ENABLED, delegationEnabled))
                .build();
    }

    private static Map<String, ColumnHandle> assignments()
    {
        return Map.of(
                "nationkey", new JdbcColumnHandle("nationkey", BIGINT_TYPE_HANDLE, BIGINT),
                "regionkey", new JdbcColumnHandle("regionkey", BIGINT_TYPE_HANDLE, BIGINT));
    }
}
