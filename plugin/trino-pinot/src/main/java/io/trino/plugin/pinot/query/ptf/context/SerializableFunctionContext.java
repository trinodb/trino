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
package io.trino.plugin.pinot.query.ptf.context;

import org.apache.pinot.common.request.context.FunctionContext;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public record SerializableFunctionContext(FunctionContext.Type type, String name, List<SerializableExpressionContext> arguments)
{
    public SerializableFunctionContext
    {
        requireNonNull(type, "type is null");
        requireNonNull(name, "name is null");
        requireNonNull(arguments, "arguments is null");
    }

    public FunctionContext toFunctionContext()
    {
        return new FunctionContext(type, name, arguments.stream()
                .map(SerializableExpressionContext::toExpressionContext)
                .collect(toImmutableList()));
    }
}
