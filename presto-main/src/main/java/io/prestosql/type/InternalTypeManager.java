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
package io.prestosql.type;

import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class InternalTypeManager
        implements TypeManager
{
    private final Metadata metadata;

    @Inject
    public InternalTypeManager(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return metadata.getType(signature);
    }

    @Override
    public Type fromSqlType(String type)
    {
        return metadata.fromSqlType(type);
    }

    @Override
    public Type getType(TypeId id)
    {
        return metadata.getType(id);
    }

    @Override
    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes, InvocationConvention invocationConvention)
    {
        return metadata.getScalarFunctionInvoker(metadata.resolveOperator(operatorType, argumentTypes), Optional.of(invocationConvention)).getMethodHandle();
    }
}
