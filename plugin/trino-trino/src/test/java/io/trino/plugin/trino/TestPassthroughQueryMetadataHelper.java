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

import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

final class TestPassthroughQueryMetadataHelper
{
    private static final TypeManager TYPE_MANAGER = new TestingFunctionResolution().getPlannerContext().getTypeManager();

    @Test
    void testDeallocateFailureDoesNotMaskDescribeFailure()
    {
        SQLException describeFailure = new SQLException("describe failed");
        SQLException deallocateFailure = new SQLException("deallocate failed");
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                (_, method, arguments) -> switch (method.getName()) {
                    case "execute" -> {
                        String sql = (String) arguments[0];
                        if (sql.startsWith("DEALLOCATE PREPARE")) {
                            throw deallocateFailure;
                        }
                        yield true;
                    }
                    case "executeQuery" -> throw describeFailure;
                    case "close" -> null;
                    default -> defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (_, method, _) -> method.getName().equals("createStatement")
                        ? statement
                        : defaultValue(method.getReturnType()));
        PassthroughQueryMetadataHelper helper = new PassthroughQueryMetadataHelper(
                TYPE_MANAGER,
                (_, _, _) -> Optional.empty());

        Throwable failure = catchThrowable(() -> helper.describeOutputColumns(
                connection,
                new PreparedQuery("SELECT 1", List.of())));

        assertThat(failure).isSameAs(describeFailure);
        assertThat(failure.getSuppressed()).containsExactly(deallocateFailure);
    }

    @Test
    void testPassthroughQueryNormalizationPreservesParameters()
    {
        QueryParameter parameter = new QueryParameter(BIGINT, Optional.of(1L));
        PreparedQuery query = new PreparedQuery("  SELECT ?;  ", List.of(parameter));

        PreparedQuery normalized = TrinoClient.normalizePassthroughQuery(query);

        assertThat(normalized.query()).isEqualTo("SELECT ?");
        assertThat(normalized.parameters()).containsExactly(parameter);
    }

    @Test
    void testFallbackFailurePreservesPrimaryFailure()
    {
        SQLException primaryFailure = new SQLException("metadata failed");
        IllegalStateException fallbackFailure = new IllegalStateException("fallback failed");

        Throwable failure = catchThrowable(() -> TrinoClient.executeFallback(
                primaryFailure,
                () -> {
                    throw fallbackFailure;
                }));

        assertThat(failure).isSameAs(fallbackFailure);
        assertThat(failure.getSuppressed()).containsExactly(primaryFailure);
    }

    private static Object defaultValue(Class<?> type)
    {
        if (!type.isPrimitive() || type == void.class) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }
}
