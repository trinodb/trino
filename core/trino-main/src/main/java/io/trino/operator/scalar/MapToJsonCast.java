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
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.SqlOperator;
import io.trino.spi.block.Block;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.Signature.castableToTypeParameter;
import static io.trino.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.JsonGeneratorWriter;
import static io.trino.util.JsonUtil.ObjectKeyProvider;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static io.trino.util.Reflection.methodHandle;

public class MapToJsonCast
        extends SqlOperator
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast(false);
    public static final MapToJsonCast LEGACY_MAP_TO_JSON = new MapToJsonCast(true);

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapToJsonCast.class, "toJson", ObjectKeyProvider.class, JsonGeneratorWriter.class, Block.class);

    private final boolean legacyRowToJson;

    private MapToJsonCast(boolean legacyRowToJson)
    {
        super(OperatorType.CAST,
                ImmutableList.of(
                        castableToTypeParameter("K", VARCHAR.getTypeSignature()),
                        castableToTypeParameter("V", JSON.getTypeSignature())),
                ImmutableList.of(),
                JSON.getTypeSignature(),
                ImmutableList.of(mapType(new TypeSignature("K"), new TypeSignature("V"))),
                false);
        this.legacyRowToJson = legacyRowToJson;
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        checkArgument(functionBinding.getArity() == 1, "Expected arity to be 1");
        Type keyType = functionBinding.getTypeVariable("K");
        Type valueType = functionBinding.getTypeVariable("V");
        Type mapType = functionBinding.getBoundSignature().getArgumentTypes().get(0);
        checkCondition(canCastToJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", mapType);

        ObjectKeyProvider provider = ObjectKeyProvider.createObjectKeyProvider(keyType);
        JsonGeneratorWriter writer = JsonGeneratorWriter.createJsonGeneratorWriter(valueType, legacyRowToJson);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(provider).bindTo(writer);

        return new ChoicesScalarFunctionImplementation(
                functionBinding,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Slice toJson(ObjectKeyProvider provider, JsonGeneratorWriter writer, Block block)
    {
        try {
            Map<String, Integer> orderedKeyToValuePosition = new TreeMap<>();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                String objectKey = provider.getObjectKey(block, i);
                orderedKeyToValuePosition.put(objectKey, i + 1);
            }

            SliceOutput output = new DynamicSliceOutput(40);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartObject();
                for (Map.Entry<String, Integer> entry : orderedKeyToValuePosition.entrySet()) {
                    jsonGenerator.writeFieldName(entry.getKey());
                    writer.writeJsonValue(jsonGenerator, block, entry.getValue());
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
