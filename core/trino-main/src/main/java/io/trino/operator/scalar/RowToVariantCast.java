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
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.variant.Variant;
import io.trino.util.variant.VariantWriter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.invoke.MethodType.methodType;

public class RowToVariantCast
        extends SqlScalarFunction
{
    public static final RowToVariantCast ROW_TO_VARIANT = new RowToVariantCast();

    private static final MethodHandle METHOD_HANDLE;

    static {
        try {
            METHOD_HANDLE = MethodHandles.lookup().findStatic(RowToVariantCast.class, "toVariant", methodType(Variant.class, VariantWriter.class, SqlRow.class));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private RowToVariantCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .typeVariableConstraint(
                                // this is technically a recursive constraint for cast, but TypeRegistry.canCast has explicit handling for row to variant cast
                                TypeVariableConstraint.builder("T")
                                        .rowType()
                                        .build())
                        .returnType(VARIANT)
                        .argumentType(new TypeSignature("T"))
                        .build())
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type type = boundSignature.getArgumentType(0);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(VariantWriter.create(type));
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Variant toVariant(VariantWriter variantWriter, SqlRow sqlRow)
    {
        return variantWriter.write(sqlRow);
    }
}
