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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;

class TestingH2JdbcClient
        extends BaseJdbcClient
{
    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(Types.BIGINT, Optional.empty(), -1, Optional.empty(), Optional.empty(), Optional.empty());

    public TestingH2JdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);
    }

    @Override
    public boolean supportsGroupingSets()
    {
        return false;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return new AggregateFunctionRewriter(this::quoted, ImmutableSet.of(new ImplementCountAll(BIGINT_TYPE_HANDLE)))
                .rewrite(session, aggregate, assignments);
    }
}
