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
import io.trino.operator.scalar.JsonExtract.JsonExtractor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeSignature;
import io.trino.type.JsonPathType;
import io.trino.util.JsonCastException;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.createJsonParser;
import static io.trino.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class JsonStringArrayExtractScalar
        extends SqlScalarFunction
{
    public static final JsonStringArrayExtractScalar JSON_STRING_ARRAY_EXTRACT_SCALAR = new JsonStringArrayExtractScalar();
    public static final String JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME = "$internal$json_string_array_extract_scalar";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonStringArrayExtractScalar.class, "extract", Slice.class, JsonPath.class);

    private static final JsonFactory JSON_FACTORY = createJsonFactory();
    private static final ArrayType ARRAY_TYPE = new ArrayType(VARCHAR);

    static {
        // Changes factory. Necessary for JsonParser.readValueAsTree to work.
        new ObjectMapper(JSON_FACTORY);
    }

    private JsonStringArrayExtractScalar()
    {
        super(FunctionMetadata.scalarBuilder(JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME)
                .signature(Signature.builder()
                        .argumentType(new TypeSignature("varchar", typeVariable("N")))
                        .longVariable("N")
                        .argumentType(new TypeSignature(JsonPathType.NAME))
                        .returnType(arrayType(VARCHAR.getTypeSignature()))
                        .build())
                .nullable()
                .hidden()
                .noDescription()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE);
    }

    @UsedByGeneratedCode
    public static Block extract(Slice json, JsonPath jsonPath)
    {
        try (JsonParser jsonParser = createJsonParser(JSON_FACTORY, json)) {
            jsonParser.nextToken();
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            BlockBuilder blockBuilder = ARRAY_TYPE.createBlockBuilder(null, 1);
            append(jsonParser, jsonPath.getScalarExtractor(), blockBuilder);
            Block block = blockBuilder.build();
            return ARRAY_TYPE.getObject(block, 0);
        }
        catch (TrinoException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s. %s\n%s", ARRAY_TYPE, e.getMessage(), truncateIfNecessaryForErrorMessage(json)), e);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s.\n%s", ARRAY_TYPE, truncateIfNecessaryForErrorMessage(json)), e);
        }
    }

    public static void append(JsonParser parser, JsonExtractor<Slice> extractor, BlockBuilder blockBuilder)
            throws IOException
    {
        if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
            append(null, blockBuilder);
            return;
        }

        if (parser.getCurrentToken() != START_ARRAY) {
            throw new JsonCastException(format("Expected a json array, but got %s", parser.getText()));
        }

        ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
            while (parser.nextToken() != END_ARRAY) {
                if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                    append(null, elementBuilder);
                    continue;
                }
                JsonParser jsonParser = parser.readValueAsTree().traverse();
                append(JsonExtract.extract(jsonParser, extractor), elementBuilder);
            }
        });
    }

    private static void append(Slice slice, BlockBuilder blockBuilder)
    {
        if (slice == null) {
            blockBuilder.appendNull();
        }
        else {
            VARCHAR.writeSlice(blockBuilder, slice);
        }
    }
}
