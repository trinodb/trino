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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.json.JsonInputErrorNode.JSON_ERROR;
import static io.trino.json.ir.SqlJsonLiteralConverter.getJsonNode;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.JSON_2016;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.util.Reflection.methodHandle;

public class JsonObjectFunction
        extends SqlScalarFunction
{
    public static final JsonObjectFunction JSON_OBJECT_FUNCTION = new JsonObjectFunction();
    public static final String JSON_OBJECT_FUNCTION_NAME = "$json_object";
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonObjectFunction.class, "jsonObject", RowType.class, RowType.class, Block.class, Block.class, boolean.class, boolean.class);
    private static final JsonNode EMPTY_OBJECT = new ObjectNode(JsonNodeFactory.instance);

    private JsonObjectFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(JSON_OBJECT_FUNCTION_NAME)
                        .typeVariable("K")
                        .typeVariable("V")
                        .returnType(new TypeSignature(JSON_2016))
                        .argumentTypes(ImmutableList.of(new TypeSignature("K"), new TypeSignature("V"), new TypeSignature(BOOLEAN), new TypeSignature(BOOLEAN)))
                        .build())
                .argumentNullability(true, true, false, false)
                .hidden()
                .description("Creates a JSON object from key-value pairs")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        RowType keysRowType = (RowType) boundSignature.getArgumentType(0);
        RowType valuesRowType = (RowType) boundSignature.getArgumentType(1);
        checkArgument(keysRowType.getFields().size() == valuesRowType.getFields().size(), "keys and values do not match");
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(keysRowType)
                .bindTo(valuesRowType);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static JsonNode jsonObject(RowType keysRowType, RowType valuesRowType, Block keysRow, Block valuesRow, boolean nullOnNull, boolean uniqueKeys)
    {
        if (JSON_NO_PARAMETERS_ROW_TYPE.equals(keysRowType)) {
            return EMPTY_OBJECT;
        }

        Map<String, JsonNode> members = new HashMap<>();
        List<Block> keys = keysRow.getChildren();
        List<Block> values = valuesRow.getChildren();

        for (int i = 0; i < keysRowType.getFields().size(); i++) {
            Type keyType = keysRowType.getFields().get(i).getType();
            Object key = readNativeValue(keyType, keys.get(i), 0);
            if (key == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "null value passed for JSON object key to JSON_OBJECT function");
            }
            String keyName = ((Slice) key).toStringUtf8();

            Type valueType = valuesRowType.getFields().get(i).getType();
            Object value = readNativeValue(valueType, values.get(i), 0);
            checkState(!JSON_ERROR.equals(value), "malformed JSON error suppressed in the input function");

            JsonNode valueNode;
            if (value == null) {
                if (nullOnNull) {
                    valueNode = NullNode.getInstance();
                }
                else { // absent on null
                    continue;
                }
            }
            else if (valueType.equals(Json2016Type.JSON_2016)) {
                valueNode = (JsonNode) value;
            }
            else {
                valueNode = getJsonNode(TypedValue.fromValueAsObject(valueType, value))
                        .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "value passed to JSON_OBJECT function cannot be converted to JSON"));
            }

            if (members.put(keyName, valueNode) != null) {
                if (uniqueKeys) {
                    // failure is the expected behavior when a duplicate key is found in the WITH UNIQUE KEYS option
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "duplicate key passed to JSON_OBJECT function");
                }
                // in the WITHOUT UNIQUE KEYS option, if a duplicate key is found, both entries should be present in the resulting JSON object (per SQL standard p. 359-360).
                // the chosen library does not support JSON objects with duplicate keys.
                // we try to support the WITHOUT UNIQUE KEYS option, which is the default, but we have to fail if a duplicate key appears.
                throw new TrinoException(NOT_SUPPORTED, "cannot construct a JSON object with duplicate key");
            }
        }

        return new ObjectNode(JsonNodeFactory.instance, members);
    }
}
