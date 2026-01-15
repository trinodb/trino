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
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;

public final class RowTransformFunction
        extends SqlScalarFunction
{
    public static final RowTransformFunction ROW_TRANSFORM_FUNCTION = new RowTransformFunction();
    private static final String ROW_TRANSFORM_NAME = "transform";
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowTransformFunction.class, "transform", RowType.class, Type.class, SqlRow.class, Slice.class, Object.class, UnaryFunctionInterface.class);

    private RowTransformFunction()
    {
        super(FunctionMetadata.scalarBuilder(ROW_TRANSFORM_NAME)
                .signature(Signature.builder()
                        .variadicTypeParameter("T", "row")
                        .typeVariable("V")
                        .returnType(new TypeSignature("T"))
                        .argumentType(new TypeSignature("T"))
                        .argumentType(VARCHAR.getTypeSignature())
                        .argumentType(new TypeSignature("V"))
                        .argumentType(functionType(new TypeSignature("V"), new TypeSignature("V")))
                        .build())
                .description("Apply lambda to the value of a field, returning the transformed row")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        RowType rowType = (RowType) boundSignature.getArgumentType(0);
        Type valueType = boundSignature.getArgumentType(2);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL, NEVER_NULL, FUNCTION),
                ImmutableList.of(UnaryFunctionInterface.class),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeParameterType(4, valueType.getJavaType())
                ).bindTo(rowType).bindTo(valueType),
                Optional.empty());
    }

    @UsedByGeneratedCode
    public static SqlRow transform(RowType rowType, Type valueType, SqlRow sqlRow, Slice fieldNameSlice, Object dummyValue, UnaryFunctionInterface function)
    {
        int fieldIndex = -1;
        Field match = null;
        String fieldName = fieldNameSlice.toStringUtf8();
        List<Field> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (field.getName().orElse("").equals(fieldName)) {
                match = field;
                fieldIndex = i;
                break;
            }
        }

        if (match == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, String.format("Field with name %s not found in row", fieldName));
        }
        if (match.getType().getClass() != valueType.getClass()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, String.format("Incompatible function types: field is of type %s but lambda returns %s", match.getType(), valueType));
        }

        Block[] blocks = new Block[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            if (i != fieldIndex) {
                blocks[i] = sqlRow.getRawFieldBlock(i).getSingleValueBlock(sqlRow.getRawIndex());
            }
            else {
                Object value = readNativeValue(valueType, sqlRow.getRawFieldBlock(i), sqlRow.getRawIndex());
                blocks[i] = writeNativeValue(valueType, function.apply(value));
            }
        }
        return new SqlRow(0, blocks);
    }
}
