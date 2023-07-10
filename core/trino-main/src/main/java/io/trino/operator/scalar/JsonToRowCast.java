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
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeSignature;
import io.trino.util.JsonCastException;
import io.trino.util.JsonUtil.BlockBuilderAppender;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.BlockBuilderAppender.createBlockBuilderAppender;
import static io.trino.util.JsonUtil.canCastFromJson;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.createJsonParser;
import static io.trino.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public class JsonToRowCast
        extends SqlScalarFunction
{
    public static final JsonToRowCast JSON_TO_ROW = new JsonToRowCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToRowCast.class, "toRow", RowType.class, BlockBuilderAppender.class, ConnectorSession.class, Slice.class);

    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    static {
        // Changes factory. Necessary for JsonParser.readValueAsTree to work.
        new ObjectMapper(JSON_FACTORY);
    }

    private JsonToRowCast()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(CAST)
                        .typeVariableConstraint(
                                // this is technically a recursive constraint for cast, but TypeRegistry.canCast has explicit handling for json to row cast
                                TypeVariableConstraint.builder("T")
                                        .variadicBound("row")
                                        .castableFrom(JSON)
                                        .build())
                        .returnType(new TypeSignature("T"))
                        .argumentType(JSON)
                        .build())
                .nullable()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        RowType rowType = (RowType) boundSignature.getReturnType();
        checkCondition(canCastFromJson(rowType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", rowType);

        BlockBuilderAppender fieldAppender = createBlockBuilderAppender(rowType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(rowType).bindTo(fieldAppender);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Block toRow(
            RowType rowType,
            BlockBuilderAppender rowAppender,
            ConnectorSession connectorSession,
            Slice json)
    {
        try (JsonParser jsonParser = createJsonParser(JSON_FACTORY, json)) {
            jsonParser.nextToken();
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }

            BlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
            rowAppender.append(jsonParser, rowBlockBuilder);
            if (jsonParser.nextToken() != null) {
                throw new JsonCastException(format("Unexpected trailing token: %s", jsonParser.getText()));
            }
            return rowType.getObject(rowBlockBuilder, 0);
        }
        catch (TrinoException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s. %s\n%s", rowType, e.getMessage(), truncateIfNecessaryForErrorMessage(json)), e);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s.\n%s", rowType, truncateIfNecessaryForErrorMessage(json)), e);
        }
    }
}
