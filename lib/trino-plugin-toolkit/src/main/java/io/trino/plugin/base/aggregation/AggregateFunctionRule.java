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
package io.trino.plugin.base.aggregation;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;

public interface AggregateFunctionRule<Result>
{
    Pattern<AggregateFunction> getPattern();

    Optional<Result> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context);

    interface RewriteContext
    {
        default ColumnHandle getAssignment(String name)
        {
            requireNonNull(name, "name is null");
            ColumnHandle columnHandle = getAssignments().get(name);
            verifyNotNull(columnHandle, "No assignment for %s", name);
            return columnHandle;
        }

        Map<String, ColumnHandle> getAssignments();

        Function<String, String> getIdentifierQuote();

        ConnectorSession getSession();
    }
}
