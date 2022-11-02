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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.util.JsonUtil.JsonGeneratorWriter;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static io.trino.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.JsonGeneratorWriter.createJsonGeneratorWriter;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static io.trino.util.Reflection.methodHandle;

public class RowToJsonCast
        extends SqlScalarFunction
{
    public static final RowToJsonCast ROW_TO_JSON = new RowToJsonCast();

    private static final MethodHandle METHOD_HANDLE = methodHandle(RowToJsonCast.class, "toJsonObject", List.class, List.class, Block.class);

    private RowToJsonCast()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(CAST)
                        .typeVariableConstraint(
                                // this is technically a recursive constraint for cast, but TypeRegistry.canCast has explicit handling for row to json cast
                                TypeVariableConstraint.builder("T")
                                        .variadicBound("row")
                                        .castableTo(JSON)
                                        .build())
                        .returnType(JSON)
                        .argumentType(new TypeSignature("T"))
                        .build())
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type type = boundSignature.getArgumentType(0);
        checkCondition(canCastToJson(type), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", type);

        List<Type> fieldTypes = type.getTypeParameters();

        List<JsonGeneratorWriter> fieldWriters = new ArrayList<>(fieldTypes.size());
        List<TypeSignatureParameter> typeSignatureParameters = type.getTypeSignature().getParameters();
        List<String> fieldNames = new ArrayList<>(fieldTypes.size());
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldNames.add(typeSignatureParameters.get(i).getNamedTypeSignature().getName().orElse(""));
            fieldWriters.add(createJsonGeneratorWriter(fieldTypes.get(i)));
        }
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(fieldNames).bindTo(fieldWriters);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Slice toJsonObject(List<String> fieldNames, List<JsonGeneratorWriter> fieldWriters, Block block)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(40);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartObject();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    jsonGenerator.writeFieldName(fieldNames.get(i));
                    fieldWriters.get(i).writeJsonValue(jsonGenerator, block, i);
                }
                jsonGenerator.writeEndObject();
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
