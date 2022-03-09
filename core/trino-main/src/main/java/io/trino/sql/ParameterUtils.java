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
package io.trino.sql;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.ParameterExtractor.getParameters;

public class ParameterUtils
{
    private ParameterUtils() {}

    public static Map<NodeRef<Parameter>, Expression> parameterExtractor(Statement statement, List<Expression> parameters)
    {
        List<Parameter> parametersList = getParameters(statement).stream()
                .sorted(Comparator.comparing(
                        parameter -> parameter.getLocation().get(),
                        Comparator.comparing(NodeLocation::getLineNumber)
                                .thenComparing(NodeLocation::getColumnNumber)))
                .collect(toImmutableList());

        ImmutableMap.Builder<NodeRef<Parameter>, Expression> builder = ImmutableMap.builder();
        Iterator<Expression> iterator = parameters.iterator();
        for (Parameter parameter : parametersList) {
            builder.put(NodeRef.of(parameter), iterator.next());
        }
        return builder.buildOrThrow();
    }
}
