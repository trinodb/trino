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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.operator.OperatorFactory;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class TypedOperatorFactory
{
    private final OperatorFactory operatorFactory;
    private final List<Type> types;

    public TypedOperatorFactory(OperatorFactory operatorFactory, List<Type> types)
    {
        this.operatorFactory = requireNonNull(operatorFactory, "operatorFactory is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    public TypedOperatorFactory duplicate()
    {
        return new TypedOperatorFactory(operatorFactory.duplicate(), types);
    }

    public OperatorFactory operatorFactory()
    {
        return operatorFactory;
    }

    public List<Type> types()
    {
        return types;
    }

    @Override
    public String toString()
    {
        return "TypedOperatorFactory[" +
                "operatorFactory=" + operatorFactory + ", " +
                "types=" + types + ']';
    }
}
