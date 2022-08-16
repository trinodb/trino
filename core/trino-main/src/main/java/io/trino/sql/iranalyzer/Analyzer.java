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
package io.trino.sql.iranalyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.GroupingOperation;

import java.util.List;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.sql.iranalyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.iranalyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.iranalyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.iranalyzer.SemanticExceptions.semanticException;

public class Analyzer
{
    static void verifyNoAggregateWindowOrGroupingFunctions(Session session, Metadata metadata, Expression predicate, String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), session, metadata);

        List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate));

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw semanticException(EXPRESSION_NOT_SCALAR, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }

    private Analyzer()
    {}
}
