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
package io.trino.operator.scalar.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.json.ir.TypedValue;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.Json2016Type;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.json.ir.SqlJsonLiteralConverter.getJsonNode;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.util.Reflection.methodHandle;

public class JsonArrayFunction
        extends SqlScalarFunction
{
    public static final JsonArrayFunction JSON_ARRAY_FUNCTION = new JsonArrayFunction();
    public static final String JSON_ARRAY_FUNCTION_NAME = "$json_array";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonArrayFunction.class, "jsonArray", RowType.class, Block.class, boolean.class);
    private static final JsonNode EMPTY_ARRAY = new ArrayNode(JsonNodeFactory.instance);

    private JsonArrayFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(JSON_ARRAY_FUNCTION_NAME)
                        .typeVariable("E")
                        .returnType(new TypeSignature(JSON_2016))
                        .argumentTypes(ImmutableList.of(new TypeSignature("E"), new TypeSignature(BOOLEAN)))
                        .build())
                .argumentNullability(true, false)
                .hidden()
                .description("Creates a JSON array from elements")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        RowType elementsRowType = (RowType) boundSignature.getArgumentType(0);
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(elementsRowType);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(BOXED_NULLABLE, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static JsonNode jsonArray(RowType elementsRowType, Block elementsRow, boolean nullOnNull)
    {
        if (JSON_NO_PARAMETERS_ROW_TYPE.equals(elementsRowType)) {
            return EMPTY_ARRAY;
        }

        List<Block> elements = elementsRow.getChildren();
        ImmutableList.Builder<JsonNode> arrayElements = ImmutableList.builder();

        for (int i = 0; i < elementsRowType.getFields().size(); i++) {
            Type elementType = elementsRowType.getFields().get(i).getType();
            Object element = readNativeValue(elementType, elements.get(i), 0);
            checkState(!JSON_ERROR.equals(element), "malformed JSON error suppressed in the input function");

            JsonNode elementNode;
            if (element == null) {
                if (nullOnNull) {
                    elementNode = NullNode.getInstance();
                }
                else { // absent on null
                    continue;
                }
            }
            else if (elementType.equals(Json2016Type.JSON_2016)) {
                elementNode = (JsonNode) element;
            }
            else {
                elementNode = getJsonNode(TypedValue.fromValueAsObject(elementType, element))
                        .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "value passed to JSON_ARRAY function cannot be converted to JSON"));
            }

            arrayElements.add(elementNode);
        }

        return new ArrayNode(JsonNodeFactory.instance, arrayElements.build());
    }
}
