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
package io.trino.plugin.sqlserver;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.spi.connector.AggregateFunction;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.inputs;
import static io.trino.plugin.sqlserver.SqlServerClient.BIGINT_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Implements specialized version of {@code count(*)} that returns bigint in SQL Server.
 */
public class ImplementSqlServerCountBigAll
        implements AggregateFunctionRule<JdbcExpression>
{
    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("count"))
                .with(inputs().equalTo(List.of()));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        verify(aggregateFunction.getOutputType() == BIGINT);
        return Optional.of(new JdbcExpression("count_big(*)", BIGINT_TYPE));
    }
}
