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
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonNestingDepthException;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.util.JsonUtil.JsonValueWriter;
import io.trino.util.JsonUtil.ObjectKeyProvider;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static io.trino.json.JsonItems.MAX_NESTING_DEPTH;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeTemplates.mapType;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.Reflection.methodHandle;

public class MapToJsonCast
        extends SqlScalarFunction
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapToJsonCast.class, "toJson", ObjectKeyProvider.class, JsonValueWriter.class, SqlMap.class);

    private MapToJsonCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .castableToTypeParameter("K", VARCHAR.getTypeDescriptor())
                        .castableToTypeParameter("V", JSON.getTypeDescriptor())
                        .returnType(JSON)
                        .argumentType(mapType(typeVariable("K"), typeVariable("V")))
                        .build())
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        MapType mapType = (MapType) boundSignature.getArgumentType(0);
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        checkCondition(canCastToJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", mapType);

        ObjectKeyProvider provider = ObjectKeyProvider.createObjectKeyProvider(keyType);
        JsonValueWriter writer = JsonValueWriter.createJsonValueWriter(valueType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(provider).bindTo(writer);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Json toJson(ObjectKeyProvider provider, JsonValueWriter writer, SqlMap map)
    {
        int rawOffset = map.getRawOffset();
        Block rawKeyBlock = map.getRawKeyBlock();
        Block rawValueBlock = map.getRawValueBlock();
        Map<String, Integer> orderedKeyToValuePosition = new TreeMap<>();
        for (int index = 0; index < map.getSize(); index++) {
            orderedKeyToValuePosition.put(provider.getObjectKey(rawKeyBlock, rawOffset + index), rawOffset + index);
        }
        try {
            return JsonItemBuilder.encodeWithDepthLimit(jsonWriter -> {
                jsonWriter.startObject();
                for (Entry<String, Integer> entry : orderedKeyToValuePosition.entrySet()) {
                    jsonWriter.fieldName(entry.getKey());
                    writer.writeJsonValue(jsonWriter, rawValueBlock, entry.getValue());
                }
                jsonWriter.endObject();
            }, MAX_NESTING_DEPTH);
        }
        catch (JsonNestingDepthException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, e.getMessage(), e);
        }
    }
}
