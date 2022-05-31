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

import com.fasterxml.jackson.databind.node.NullNode;
import io.trino.json.ir.TypedValue;
import io.trino.spi.block.Block;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.Json2016Type;

import java.util.List;

import static io.trino.json.JsonEmptySequenceNode.EMPTY_SEQUENCE;
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
     * All non-null values are passed as-is. Conversions apply in the following cases:
     * - null value with FORMAT option is converted into an empty JSON sequence
     * - null value without FORMAT option is converted into a JSON null.
     *
     * @param parametersRowType type of the Block containing parameters
     * @param parametersRow a Block containing parameters
     * @return an array containing the converted values
     */
    public static Object[] getParametersArray(Type parametersRowType, Block parametersRow)
    {
        if (JSON_NO_PARAMETERS_ROW_TYPE.equals(parametersRowType)) {
            return new Object[] {};
        }

        RowType rowType = (RowType) parametersRowType;
        List<Block> parameterBlocks = parametersRow.getChildren();

        Object[] array = new Object[rowType.getFields().size()];
        for (int i = 0; i < rowType.getFields().size(); i++) {
            Type type = rowType.getFields().get(i).getType();
            Object value = readNativeValue(type, parameterBlocks.get(i), 0);
            if (type.equals(Json2016Type.JSON_2016)) {
                if (value == null) {
                    array[i] = EMPTY_SEQUENCE; // null as JSON value shall produce an empty sequence
                }
                else {
                    array[i] = value;
                }
            }
            else if (value == null) {
                array[i] = NullNode.getInstance(); // null as a non-JSON value shall produce a JSON null
            }
            else {
                array[i] = TypedValue.fromValueAsObject(type, value);
            }
        }

        return array;
    }
}
