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

import com.google.common.base.Joiner;
import io.prestosql.spi.type.Type;

import java.util.List;

public class FunctionMetadata
{
    private final String name;
    private final FunctionKind kind;
    private final Type returnType;
    private final List<Type> argumentTypes;

    public FunctionMetadata(String name, FunctionKind kind, Type returnType, List<Type> argumentTypes)
    {
        this.name = name;
        this.kind = kind;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
    }

    public String getName()
    {
        return name;
    }

    public FunctionKind getKind()
    {
        return kind;
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public String toString()
    {
        return name + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }
}
