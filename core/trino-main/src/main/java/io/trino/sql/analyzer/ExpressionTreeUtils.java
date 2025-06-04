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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.FunctionResolver;
import io.trino.security.AccessControl;
import io.trino.spi.Location;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WindowOperation;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class ExpressionTreeUtils
{
    private ExpressionTreeUtils() {}

    static List<FunctionCall> extractAggregateFunctions(Iterable<? extends Node> nodes, Session session, FunctionResolver functionResolver, AccessControl accessControl)
    {
        return extractExpressions(nodes, FunctionCall.class, function -> isAggregation(function, session, functionResolver, accessControl));
    }

    static List<Expression> extractWindowExpressions(Iterable<? extends Node> nodes)
    {
        return ImmutableList.<Expression>builder()
                .addAll(extractWindowFunctions(nodes))
                .addAll(extractWindowMeasures(nodes))
                .build();
    }

    static List<Expression> extractWindowExpressions(Iterable<? extends Node> nodes, Session session, FunctionResolver functionResolver, AccessControl accessControl)
    {
        return ImmutableList.<Expression>builder()
                .addAll(extractWindowFunctions(nodes, session, functionResolver, accessControl))
                .addAll(extractWindowMeasures(nodes))
                .build();
    }

    static List<FunctionCall> extractWindowFunctions(Iterable<? extends Node> nodes)
    {
        return extractExpressions(nodes, FunctionCall.class, ExpressionTreeUtils::isWindowFunction);
    }

    static List<FunctionCall> extractWindowFunctions(Iterable<? extends Node> nodes, Session session, FunctionResolver functionResolver, AccessControl accessControl)
    {
        return extractExpressions(nodes, FunctionCall.class, function -> isWindow(function, session, functionResolver, accessControl));
    }

    static List<WindowOperation> extractWindowMeasures(Iterable<? extends Node> nodes)
    {
        return extractExpressions(nodes, WindowOperation.class);
    }

    public static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz)
    {
        return extractExpressions(nodes, clazz, alwaysTrue());
    }

    private static boolean isAggregation(FunctionCall functionCall, Session session, FunctionResolver functionResolver, AccessControl accessControl)
    {
        return (functionResolver.isAggregationFunction(session, functionCall.getName(), accessControl)
                || functionCall.getFilter().isPresent()
                || functionCall.getOrderBy().isPresent())
                && functionCall.getWindow().isEmpty();
    }

    private static boolean isWindow(FunctionCall functionCall, Session session, FunctionResolver functionResolver, AccessControl accessControl)
    {
        return functionCall.getWindow().isPresent()
                || functionResolver.isWindowFunction(session, functionCall.getName(), accessControl);
    }

    private static boolean isWindowFunction(FunctionCall functionCall)
    {
        return functionCall.getWindow().isPresent();
    }

    private static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz,
            Predicate<T> predicate)
    {
        requireNonNull(nodes, "nodes is null");
        requireNonNull(clazz, "clazz is null");
        requireNonNull(predicate, "predicate is null");

        return stream(nodes)
                .flatMap(node -> linearizeNodes(node).stream())
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .filter(predicate)
                .collect(toImmutableList());
    }

    private static List<Node> linearizeNodes(Node node)
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Void>()
        {
            @Override
            public Void process(Node node, Void context)
            {
                super.process(node, context);
                nodes.add(node);
                return null;
            }
        }.process(node, null);
        return nodes.build();
    }

    public static Optional<Location> extractLocation(Node node)
    {
        return node.getLocation()
                .map(location -> new Location(location.getLineNumber(), location.getColumnNumber()));
    }

    public static QualifiedName asQualifiedName(Expression expression)
    {
        QualifiedName name = null;
        if (expression instanceof Identifier identifier) {
            name = QualifiedName.of(identifier.getValue());
        }
        else if (expression instanceof DereferenceExpression dereferenceExpression) {
            name = DereferenceExpression.getQualifiedName(dereferenceExpression);
        }
        return name;
    }
}
