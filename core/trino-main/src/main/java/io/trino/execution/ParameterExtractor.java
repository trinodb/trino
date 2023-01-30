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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class ParameterExtractor
{
    private ParameterExtractor() {}

    public static int getParameterCount(Statement statement)
    {
        return extractParameters(statement).size();
    }

    public static List<Parameter> extractParameters(Statement statement)
    {
        ParameterExtractingVisitor parameterExtractingVisitor = new ParameterExtractingVisitor();
        parameterExtractingVisitor.process(statement, null);
        return parameterExtractingVisitor.getParameters().stream()
                .sorted(Comparator.comparing(
                        parameter -> parameter.getLocation().get(),
                        Comparator.comparing(NodeLocation::getLineNumber)
                                .thenComparing(NodeLocation::getColumnNumber)))
                .collect(toImmutableList());
    }

    public static Map<NodeRef<Parameter>, Expression> bindParameters(Statement statement, List<Expression> values)
    {
        List<Parameter> parametersList = extractParameters(statement);

        ImmutableMap.Builder<NodeRef<Parameter>, Expression> builder = ImmutableMap.builder();
        Iterator<Expression> iterator = values.iterator();
        for (Parameter parameter : parametersList) {
            builder.put(NodeRef.of(parameter), iterator.next());
        }
        return builder.buildOrThrow();
    }

    private static class ParameterExtractingVisitor
            extends DefaultTraversalVisitor<Void>
    {
        private final List<Parameter> parameters = new ArrayList<>();

        public List<Parameter> getParameters()
        {
            return parameters;
        }

        @Override
        public Void visitParameter(Parameter node, Void context)
        {
            parameters.add(node);
            return null;
        }
    }
}
