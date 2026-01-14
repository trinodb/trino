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
import io.airlift.slice.Slice;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.trino.spi.block.RowTransformer.build;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class RowReplaceFunction
        extends SqlScalarFunction
{
    public static final RowReplaceFunction ROW_REPLACE_FUNCTION = new RowReplaceFunction();

    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = Stream.of(boolean.class, double.class, long.class, Slice.class, Object.class)
            .collect(toMap(identity(), valueType -> methodHandle(RowReplaceFunction.class, "rowReplace", RowType.class, SqlRow.class, Slice.class, valueType)));

    private RowReplaceFunction()
    {
        super(FunctionMetadata.scalarBuilder("row_replace")
                .description("Replace the value of a field in a (nested) row")
                .signature(Signature.builder()
                        .rowTypeParameter("T")
                        .typeVariable("V")
                        .returnType(new TypeSignature("T"))
                        .argumentType(new TypeSignature("T"))
                        .argumentType(VARCHAR.getTypeSignature())
                        .argumentType(new TypeSignature("V"))
                        .build())
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type type = boundSignature.getReturnType();

        if (type instanceof RowType rowType) {
            Class<?> valueType = boundSignature.getArgumentType(2).getJavaType();
            MethodHandle methodHandle = METHOD_HANDLES.get(valueType);

            return new ChoicesSpecializedSqlScalarFunction(
                    boundSignature,
                    FAIL_ON_NULL,
                    ImmutableList.of(NEVER_NULL, NEVER_NULL, NEVER_NULL),
                    methodHandle.bindTo(rowType));
        }
        throw new IllegalArgumentException("Expected a row type, but was " + type);
    }

    public static SqlRow rowReplace(RowType rowType, SqlRow row, Slice fieldPath, Object value)
    {
        return build(rowType, row, transformer -> transformer.transform((type, _) -> TypeUtils.writeNativeValue(type, value), fieldPath.toStringUtf8().split("\\.")));
    }

    public static SqlRow rowReplace(RowType rowType, SqlRow row, Slice fieldPath, boolean value)
    {
        return rowReplace(rowType, row, fieldPath, Boolean.valueOf(value));
    }

    public static SqlRow rowReplace(RowType rowType, SqlRow row, Slice fieldPath, double value)
    {
        return rowReplace(rowType, row, fieldPath, Double.valueOf(value));
    }

    public static SqlRow rowReplace(RowType rowType, SqlRow row, Slice fieldPath, long value)
    {
        return rowReplace(rowType, row, fieldPath, Long.valueOf(value));
    }

    public static SqlRow rowReplace(RowType rowType, SqlRow row, Slice fieldPath, Slice value)
    {
        return rowReplace(rowType, row, fieldPath, (Object) value);
    }
}
