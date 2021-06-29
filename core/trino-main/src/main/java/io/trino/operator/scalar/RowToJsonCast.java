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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.SqlOperator;
import io.trino.metadata.TypeVariableConstraint;
import io.trino.spi.block.Block;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.util.JsonUtil.JsonGeneratorWriter;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.JsonGeneratorWriter.createJsonGeneratorWriter;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static io.trino.util.Reflection.methodHandle;

public class RowToJsonCast
        extends SqlOperator
{
    public static final RowToJsonCast ROW_TO_JSON = new RowToJsonCast(false);
    public static final RowToJsonCast LEGACY_ROW_TO_JSON = new RowToJsonCast(true);

    private static final MethodHandle METHOD_HANDLE = methodHandle(RowToJsonCast.class, "toJsonObject", List.class, List.class, Block.class);
    private static final MethodHandle LEGACY_METHOD_HANDLE = methodHandle(RowToJsonCast.class, "toJsonArray", List.class, Block.class);

    private final boolean legacyRowToJson;

    private RowToJsonCast(boolean legacyRowToJson)
    {
        super(OperatorType.CAST,
                ImmutableList.of(
                        // this is technically a recursive constraint for cast, but TypeRegistry.canCast has explicit handling for row to json cast
                        new TypeVariableConstraint("T", false, false, "row", ImmutableSet.of(JSON.getTypeSignature()), ImmutableSet.of())),
                ImmutableList.of(),
                JSON.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T")),
                false);
        this.legacyRowToJson = legacyRowToJson;
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        checkArgument(functionBinding.getArity() == 1, "Expected arity to be 1");
        Type type = functionBinding.getTypeVariable("T");
        checkCondition(canCastToJson(type), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", type);

        List<Type> fieldTypes = type.getTypeParameters();

        List<JsonGeneratorWriter> fieldWriters = new ArrayList<>(fieldTypes.size());
        MethodHandle methodHandle;
        if (legacyRowToJson) {
            for (Type fieldType : fieldTypes) {
                fieldWriters.add(createJsonGeneratorWriter(fieldType, true));
            }
            methodHandle = LEGACY_METHOD_HANDLE.bindTo(fieldWriters);
        }
        else {
            List<TypeSignatureParameter> typeSignatureParameters = type.getTypeSignature().getParameters();
            List<String> fieldNames = new ArrayList<>(fieldTypes.size());
            for (int i = 0; i < fieldTypes.size(); i++) {
                fieldNames.add(typeSignatureParameters.get(i).getNamedTypeSignature().getName().orElse(""));
                fieldWriters.add(createJsonGeneratorWriter(fieldTypes.get(i), false));
            }
            methodHandle = METHOD_HANDLE.bindTo(fieldNames).bindTo(fieldWriters);
        }

        return new ChoicesScalarFunctionImplementation(
                functionBinding,
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

    @UsedByGeneratedCode
    public static Slice toJsonArray(List<JsonGeneratorWriter> fieldWriters, Block block)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(40);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartArray();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    fieldWriters.get(i).writeJsonValue(jsonGenerator, block, i);
                }
                jsonGenerator.writeEndArray();
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
