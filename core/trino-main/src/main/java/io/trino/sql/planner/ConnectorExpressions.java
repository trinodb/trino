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
package io.prestosql.sql.planner;

import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Variable;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressions
{
    private ConnectorExpressions() {}

    public static List<Variable> extractVariables(ConnectorExpression expression)
    {
        return preOrder(expression)
                .filter(Variable.class::isInstance)
                .map(Variable.class::cast)
                .collect(toImmutableList());
    }

    public static Stream<ConnectorExpression> preOrder(ConnectorExpression expression)
    {
        return stream(
                Traverser.forTree((SuccessorsFunction<ConnectorExpression>) ConnectorExpression::getChildren)
                        .depthFirstPreOrder(requireNonNull(expression, "expression is null")));
    }
}
