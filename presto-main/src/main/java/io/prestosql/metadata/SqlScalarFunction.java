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
package io.prestosql.metadata;

import io.prestosql.operator.scalar.ScalarFunctionImplementation;

public abstract class SqlScalarFunction
        implements SqlFunction
{
    private final FunctionMetadata functionMetadata;

    protected SqlScalarFunction(FunctionMetadata functionMetadata)
    {
        this.functionMetadata = functionMetadata;
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public abstract ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata);

    public static PolymorphicScalarFunctionBuilder builder(Class<?> clazz)
    {
        return new PolymorphicScalarFunctionBuilder(clazz);
    }
}
