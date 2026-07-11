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

import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

final class TrinoDelegationAnalyzer
{
    private final TrinoRemoteSqlRenderer renderer;

    TrinoDelegationAnalyzer(TrinoRemoteSqlRenderer renderer)
    {
        this.renderer = requireNonNull(renderer, "renderer is null");
    }

    Optional<ParameterizedExpression> analyzePredicate(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(capabilities, "capabilities is null");
        if (!isEnabled(session)) {
            return Optional.empty();
        }
        return renderer.renderExpression(session, expression, assignments, capabilities);
    }

    Optional<JdbcExpression> analyzeProjection(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(capabilities, "capabilities is null");
        if (!isEnabled(session)) {
            return Optional.empty();
        }
        return renderer.renderProjection(session, expression, assignments, capabilities);
    }

    Optional<JdbcExpression> analyzeAggregation(
            ConnectorSession session,
            AggregateFunction aggregate,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(capabilities, "capabilities is null");
        if (!isEnabled(session)) {
            return Optional.empty();
        }
        return renderer.renderAggregation(session, aggregate, assignments, capabilities);
    }

    private static boolean isEnabled(ConnectorSession session)
    {
        return TrinoRemoteDelegationSessionProperties.isRemoteDelegationEnabled(session);
    }
}
