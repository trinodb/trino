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

import com.fasterxml.jackson.core.JsonFactory;
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
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.TreeMap;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.JsonGeneratorWriter;
import static io.trino.util.JsonUtil.ObjectKeyProvider;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static io.trino.util.Reflection.methodHandle;

public class MapToJsonCast
        extends SqlScalarFunction
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapToJsonCast.class, "toJson", ObjectKeyProvider.class, JsonGeneratorWriter.class, Block.class);

    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    private MapToJsonCast()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(CAST)
                        .castableToTypeParameter("K", VARCHAR.getTypeSignature())
                        .castableToTypeParameter("V", JSON.getTypeSignature())
                        .returnType(JSON)
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .build())
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType mapType = (MapType) boundSignature.getArgumentType(0);
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        checkCondition(canCastToJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", mapType);

        ObjectKeyProvider provider = ObjectKeyProvider.createObjectKeyProvider(keyType);
        JsonGeneratorWriter writer = JsonGeneratorWriter.createJsonGeneratorWriter(valueType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(provider).bindTo(writer);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
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
