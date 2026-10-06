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
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.type.JsonPathType;
import io.trino.util.JsonCastException;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static io.trino.operator.scalar.JsonFunctions.jsonParse;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeDescriptor.arrayType;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class JsonStringArrayExtractScalar
        extends SqlScalarFunction
{
    public static final JsonStringArrayExtractScalar JSON_STRING_ARRAY_EXTRACT_SCALAR = new JsonStringArrayExtractScalar();
    public static final String JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME = "$internal$json_string_array_extract_scalar";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonStringArrayExtractScalar.class, "extract", MethodHandle.class, Slice.class, JsonPath.class);

    private static final ArrayType JSON_ARRAY_TYPE = new ArrayType(JSON);
    private static final ArrayType ARRAY_TYPE = new ArrayType(VARCHAR);

    private JsonStringArrayExtractScalar()
    {
        super(FunctionMetadata.scalarBuilder(JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME)
                .signature(Signature.builder()
                        .argumentType(type("varchar", numericVariable("N")))
                        .numericVariable("N")
                        .argumentType(type(JsonPathType.NAME))
                        .returnType(arrayType(VARCHAR.getTypeDescriptor()))
                        .build())
                .nullable()
                .hidden()
                .description("")
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addCast(JSON, JSON_ARRAY_TYPE)
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE.bindTo(functionDependencies.getCastImplementation(JSON, JSON_ARRAY_TYPE, simpleConvention(NULLABLE_RETURN, NEVER_NULL)).getMethodHandle()));
    }

    @UsedByGeneratedCode
    public static Block extract(MethodHandle jsonToArray, Slice json, JsonPath jsonPath)
            throws Throwable
    {
        try (JsonParser parser = JsonItems.createStreamingParser(json)) {
            Block result = null;
            if (parser.nextToken() != JsonToken.VALUE_NULL) {
                BlockBuilder blockBuilder = ARRAY_TYPE.createBlockBuilder(null, 1);
                append(parser, jsonPath, blockBuilder);
                result = ARRAY_TYPE.getObject(blockBuilder.build(), 0);
            }
            if (parser.nextToken() != null) {
                throw new JsonCastException(format("Unexpected trailing token: %s", parser.getText()));
            }
            return result;
        }
        catch (Exception _) {
            // Retry the original parse and cast to preserve validation, parse-first error
            // precedence, and diagnostics. Successful inputs are parsed only once.
            Block array = (Block) jsonToArray.invokeExact(jsonParse(json));
            if (array == null) {
                return null;
            }
            BlockBuilder result = VARCHAR.createBlockBuilder(null, array.getPositionCount());
            for (int position = 0; position < array.getPositionCount(); position++) {
                append(array.isNull(position) ? null : JsonFunctions.jsonExtractScalar((Json) JSON.getObject(array, position), jsonPath), result);
            }
            return result.build();
        }
    }

    public static void append(JsonParser parser, JsonPath jsonPath, BlockBuilder blockBuilder)
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
                append(JsonFunctions.jsonExtractScalar(JsonItems.parseItem(parser), jsonPath), elementBuilder);
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
