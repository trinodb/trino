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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.variant.Variant;
import io.trino.util.variant.VariantWriter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeParameter.typeVariable;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.variant.VariantUtil.canCastToVariant;
import static java.lang.invoke.MethodType.methodType;

public class MapToVariantCast
        extends SqlScalarFunction
{
    public static final MapToVariantCast MAP_TO_VARIANT = new MapToVariantCast();
    private static final MethodHandle METHOD_HANDLE;

    static {
        try {
            METHOD_HANDLE = MethodHandles.lookup().findStatic(MapToVariantCast.class, "toVariant", methodType(Variant.class, VariantWriter.class, SqlMap.class));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private MapToVariantCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .longVariable("N")
                        .castableToTypeParameter("V", VARIANT.getTypeSignature())
                        .returnType(VARIANT)
                        .argumentType(mapType(new TypeSignature("varchar", typeVariable("N")), new TypeSignature("V")))
                        .build())
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType mapType = (MapType) boundSignature.getArgumentType(0);
        checkCondition(canCastToVariant(mapType), INVALID_CAST_ARGUMENT, "Cannot cast %s to VARIANT", mapType);

        VariantWriter writer = VariantWriter.create(mapType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(writer);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    private static Variant toVariant(VariantWriter writer, SqlMap map)
    {
        return writer.write(map);
    }
}
