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
import io.airlift.slice.Slices;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeTemplates.arrayType;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;

public final class RowFieldsFunction
        extends SqlScalarFunction
{
    public static final String NAME = "fields";

    public static final RowFieldsFunction ROW_FIELDS_FUNCTION = new RowFieldsFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(RowFieldsFunction.class, "rowFields", RowType.class, SqlRow.class);

    private RowFieldsFunction()
    {
        super(FunctionMetadata.scalarBuilder(NAME)
                .signature(Signature.builder()
                        .rowTypeParameter("T")
                        .argumentType(typeVariable("T"))
                        .returnType(arrayType(type(StandardTypes.VARCHAR)))
                        .build())
                .argumentNullability(true)
                .description("Returns the field names of the row type.")
                .receiverType(type("row"))
                .neverFails()
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        RowType rowType = (RowType) boundSignature.getArgumentType(0);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(BOXED_NULLABLE),
                METHOD_HANDLE.bindTo(rowType));
    }

    @UsedByGeneratedCode
    public static Block rowFields(RowType rowType, SqlRow row)
    {
        List<RowType.Field> fields = rowType.getFields();
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, fields.size());
        fields.forEach(field -> field.getName().map(Slices::utf8Slice)
                .ifPresentOrElse(slice -> VARCHAR.writeSlice(builder, slice), builder::appendNull));
        return builder.build();
    }
}
