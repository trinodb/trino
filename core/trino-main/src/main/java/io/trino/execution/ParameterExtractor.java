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
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.tree.AstTraversal.forEachChild;

public final class ParameterExtractor
{
    private ParameterExtractor() {}

    public static int getParameterCount(Statement statement)
    {
        return extractParameters(statement).size();
    }

    public static List<Parameter> extractParameters(Statement statement)
    {
        List<Parameter> parameters = new ArrayList<>();
        collectParameters(statement, parameters);
        return parameters.stream()
                .sorted(Comparator.comparing(
                        parameter -> parameter.getLocation().get(),
                        Comparator.comparingInt(NodeLocation::line)
                                .thenComparingInt(NodeLocation::column)))
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

    private static void collectParameters(Node node, List<Parameter> parameters)
    {
        switch (node) {
            case Parameter parameter -> parameters.add(parameter);
            case CreateMaterializedView view -> {
                // The query is stored unbound in the view definition, so its parameters are not bindable.
                for (Property property : view.getProperties()) {
                    collectParameters(property, parameters);
                }
            }
            default -> forEachChild(node, child -> collectParameters(child, parameters));
        }
    }
}
