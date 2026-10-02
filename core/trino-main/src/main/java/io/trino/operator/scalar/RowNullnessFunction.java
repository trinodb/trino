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
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.util.Reflection.methodHandle;

/// Hidden `$row_is_null` / `$row_is_not_null`. SQL:2023 §8.8 Table 18 makes
/// those predicates independent for degree greater than 1, so they are two
/// functions rather than one plus `NOT`.
public final class RowNullnessFunction
        extends SqlScalarFunction
{
    public static final String IS_NULL_NAME = "$row_is_null";
    public static final String IS_NOT_NULL_NAME = "$row_is_not_null";

    public static final RowNullnessFunction ROW_IS_NULL_FUNCTION = new RowNullnessFunction(true);
    public static final RowNullnessFunction ROW_IS_NOT_NULL_FUNCTION = new RowNullnessFunction(false);

    private static final MethodHandle IS_NULL_HANDLE = methodHandle(RowNullnessFunction.class, "rowIsNull", SqlRow.class);
    private static final MethodHandle IS_NOT_NULL_HANDLE = methodHandle(RowNullnessFunction.class, "rowIsNotNull", SqlRow.class);

    private final boolean nullIfAllFieldsNull;

    private RowNullnessFunction(boolean nullIfAllFieldsNull)
    {
        super(metadata(nullIfAllFieldsNull ? IS_NULL_NAME : IS_NOT_NULL_NAME));
        this.nullIfAllFieldsNull = nullIfAllFieldsNull;
    }

    private static FunctionMetadata metadata(String name)
    {
        return FunctionMetadata.scalarBuilder(name)
                .signature(Signature.builder()
                        .rowTypeParameter("T")
                        .argumentType(typeVariable("T"))
                        .returnType(type(StandardTypes.BOOLEAN))
                        .build())
                .argumentNullability(true)
                .hidden()
                .neverFails()
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(BOXED_NULLABLE),
                nullIfAllFieldsNull ? IS_NULL_HANDLE : IS_NOT_NULL_HANDLE);
    }

    @UsedByGeneratedCode
    public static boolean rowIsNull(SqlRow row)
    {
        if (row == null) {
            return true;
        }
        int index = row.getRawIndex();
        for (int i = 0; i < row.getFieldCount(); i++) {
            if (!row.getRawFieldBlock(i).isNull(index)) {
                return false;
            }
        }
        return true;
    }

    @UsedByGeneratedCode
    public static boolean rowIsNotNull(SqlRow row)
    {
        if (row == null) {
            return false;
        }
        int index = row.getRawIndex();
        for (int i = 0; i < row.getFieldCount(); i++) {
            if (row.getRawFieldBlock(i).isNull(index)) {
                return false;
            }
        }
        return true;
    }
}
