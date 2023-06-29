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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeSignature;
import io.trino.util.JsonCastException;
import io.trino.util.JsonUtil.BlockBuilderAppender;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.BlockBuilderAppender.createBlockBuilderAppender;
import static io.trino.util.JsonUtil.canCastFromJson;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.createJsonParser;
import static io.trino.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public class JsonToMapCast
        extends SqlScalarFunction
{
    public static final JsonToMapCast JSON_TO_MAP = new JsonToMapCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToMapCast.class, "toMap", MapType.class, BlockBuilderAppender.class, ConnectorSession.class, Slice.class);

    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    static {
        // Changes factory. Necessary for JsonParser.readValueAsTree to work.
        new ObjectMapper(JSON_FACTORY);
    }

    private JsonToMapCast()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(CAST)
                        .castableFromTypeParameter("K", VARCHAR.getTypeSignature())
                        .castableFromTypeParameter("V", JSON.getTypeSignature())
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V")))
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
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType).bindTo(mapAppender);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Block toMap(MapType mapType, BlockBuilderAppender mapAppender, ConnectorSession connectorSession, Slice json)
    {
        try (JsonParser jsonParser = createJsonParser(JSON_FACTORY, json)) {
            jsonParser.nextToken();
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }

            BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
            mapAppender.append(jsonParser, blockBuilder);
            if (jsonParser.nextToken() != null) {
                throw new JsonCastException(format("Unexpected trailing token: %s", jsonParser.getText()));
            }
            return mapType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
        }
        catch (TrinoException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s. %s\n%s", mapType, e.getMessage(), truncateIfNecessaryForErrorMessage(json)), e);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s.\n%s", mapType, truncateIfNecessaryForErrorMessage(json)), e);
        }
    }
}
