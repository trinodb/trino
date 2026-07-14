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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.util.JsonCastException;
import io.trino.util.JsonUtil.BlockBuilderAppender;
import io.trino.util.JsonUtil.StreamingBlockBuilderAppender;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeTemplates.mapType;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.BlockBuilderAppender.createBlockBuilderAppender;
import static io.trino.util.JsonUtil.canCastFromJson;
import static io.trino.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public class JsonToMapCast
        extends SqlScalarFunction
{
    public static final JsonToMapCast JSON_TO_MAP = new JsonToMapCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToMapCast.class, "toMap", MapType.class, BlockBuilderAppender.class, StreamingBlockBuilderAppender.class, Json.class);

    private JsonToMapCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .castableFromTypeParameter("K", VARCHAR.getTypeDescriptor())
                        .castableFromTypeParameter("V", JSON.getTypeDescriptor())
                        .returnType(mapType(typeVariable("K"), typeVariable("V")))
                        .argumentType(JSON)
                        .build())
                .nullable()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        checkArgument(boundSignature.getArity() == 1, "Expected arity to be 1");
        MapType mapType = (MapType) boundSignature.getReturnType();
        checkCondition(canCastFromJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", mapType);

        BlockBuilderAppender mapAppender = createBlockBuilderAppender(mapType);
        StreamingBlockBuilderAppender streamingAppender = StreamingBlockBuilderAppender.create(mapType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType).bindTo(mapAppender).bindTo(streamingAppender);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static SqlMap toMap(MapType mapType, BlockBuilderAppender mapAppender, StreamingBlockBuilderAppender streamingAppender, Json json)
    {
        try {
            // Raw-text JSON streams straight from its bytes rather than through a throwaway tree;
            // isNull() would parse that tree, so the top-level null is read from the stream instead.
            if (streamingAppender != null && json.isRawText()) {
                try (JsonParser parser = JsonItems.createStreamingParser(json.rawText())) {
                    if (parser.nextToken() == JsonToken.VALUE_NULL) {
                        checkNoTrailingToken(parser);
                        return null;
                    }
                    BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
                    streamingAppender.append(parser, blockBuilder);
                    checkNoTrailingToken(parser);
                    return (SqlMap) mapType.getObject(blockBuilder.build(), 0);
                }
            }
            if (json.isNull()) {
                return null;
            }
            BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
            mapAppender.append(json, blockBuilder);
            Block block = blockBuilder.build();
            return (SqlMap) mapType.getObject(block, 0);
        }
        catch (TrinoException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s. %s\n%s", mapType, e.getMessage(), truncateIfNecessaryForErrorMessage(JsonItems.toText(json))), e);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s.\n%s", mapType, truncateIfNecessaryForErrorMessage(JsonItems.toText(json))), e);
        }
    }

    private static void checkNoTrailingToken(JsonParser parser)
            throws IOException
    {
        if (parser.nextToken() != null) {
            throw new JsonCastException(format("Unexpected trailing token: %s", parser.getText()));
        }
    }
}
