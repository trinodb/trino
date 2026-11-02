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

import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.TypedValue;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.JsonType;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;

public final class ParameterUtil
{
    private ParameterUtil() {}

    /**
     * Converts the parameters passed to json path into appropriate values,
     * respecting the proper SQL semantics for nulls in the context of
     * a path parameter, and collects them in an array.
     * <p>
     * Conversions apply in the following cases:
     * - null value with FORMAT option is represented as `null` in the array slot, and
     *   surfaces as an empty sequence at the IrNamedJsonVariable visitor.
     * - null value without FORMAT option is converted into a JSON null
     *   ({@link JsonItemBuilder#JSON_NULL}).
     * - JSON_ERROR sentinels (already a Json with Kind.ERROR) pass through; the
     *   boundary function detects them and applies its error behavior.
     *
     * @param parametersRowType type of the Block containing parameters
     * @param parametersRow a row containing parameters
     * @return an array containing the converted values (Json, TypedValue, or null)
     */
    public static Object[] getParametersArray(Type parametersRowType, SqlRow parametersRow)
    {
        if (JSON_NO_PARAMETERS_ROW_TYPE.equals(parametersRowType)) {
            return new Object[] {};
        }

        RowType rowType = (RowType) parametersRowType;
        int rawIndex = parametersRow.getRawIndex();

        Object[] array = new Object[rowType.getFields().size()];
        for (int i = 0; i < rowType.getFields().size(); i++) {
            Type type = rowType.getFields().get(i).getType();
            Object value = readNativeValue(type, parametersRow.getRawFieldBlock(i), rawIndex);
            if (type.equals(JsonType.JSON)) {
                if (value == null) {
                    array[i] = null; // null as JSON value shall produce an empty sequence
                }
                else {
                    array[i] = (Json) value;
                }
            }
            else if (value == null) {
                array[i] = JsonItemBuilder.JSON_NULL; // null as a non-JSON value shall produce a JSON null
            }
            else {
                array[i] = TypedValue.fromValueAsObject(type, value);
            }
        }

        return array;
    }
}
