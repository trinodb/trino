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
package io.trino.operator.annotations;

import io.trino.metadata.FunctionBinding;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class LiteralImplementationDependency
        implements ImplementationDependency
{
    private final String literalName;
    private final Class<?> type;

    public LiteralImplementationDependency(String literalName, Class<?> type)
    {
        this.literalName = requireNonNull(literalName, "literalName is null");
        this.type = requireNonNull(type, "type is null");
        checkArgument(type == long.class || type == int.class, "Literal parameter %s must be long or int, but is %s", literalName, type);
    }

    @Override
    public void declareDependencies(FunctionDependencyDeclarationBuilder builder) {}

    @Override
    public Object resolve(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        long value = functionBinding.variables().getNumericVariable(literalName);
        if (type == int.class) {
            return toIntExact(value);
        }
        return value;
    }
}
