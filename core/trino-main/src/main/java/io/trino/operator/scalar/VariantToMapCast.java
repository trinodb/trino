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
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.variant.Variant;
import io.trino.util.variant.VariantUtil.BlockBuilderAppender;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.variant.VariantUtil.canCastFromVariant;
import static java.lang.invoke.MethodType.methodType;

public class VariantToMapCast
        extends SqlScalarFunction
{
    public static final VariantToMapCast VARIANT_TO_MAP = new VariantToMapCast();
    private static final MethodHandle METHOD_HANDLE;

    static {
        try {
            METHOD_HANDLE = MethodHandles.lookup().findStatic(VariantToMapCast.class, "toMap", methodType(SqlMap.class, MapType.class, BlockBuilderAppender.class, ConnectorSession.class, Variant.class));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private VariantToMapCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .castableFromTypeParameter("K", VARCHAR.getTypeSignature())
                        .castableFromTypeParameter("V", VARIANT.getTypeSignature())
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(VARIANT)
                        .build())
                .nullable()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        checkArgument(boundSignature.getArity() == 1, "Expected arity to be 1");
        MapType mapType = (MapType) boundSignature.getReturnType();
        checkCondition(canCastFromVariant(mapType), INVALID_CAST_ARGUMENT, "Cannot cast VARIANT to %s", mapType);

        BlockBuilderAppender mapAppender = BlockBuilderAppender.createBlockBuilderAppender(mapType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType).bindTo(mapAppender);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static SqlMap toMap(MapType mapType, BlockBuilderAppender mapAppender, ConnectorSession connectorSession, Variant variant)
    {
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
        mapAppender.append(variant, blockBuilder);
        Block block = blockBuilder.build();
        return mapType.getObject(block, 0);
    }
}
