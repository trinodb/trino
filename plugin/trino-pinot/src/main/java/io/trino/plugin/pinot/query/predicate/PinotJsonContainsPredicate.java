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
package io.trino.plugin.pinot.query.predicate;

import io.airlift.slice.Slice;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;

import java.util.ArrayList;
import java.util.List;

public class PinotJsonContainsPredicate
        implements PinotPredicate
{
    private static final FunctionName CONTAINS = new FunctionName("contains");
    private static final FunctionName CAST = new FunctionName("$cast");
    private final String columnName;
    private final String jsonPath;
    private final List<String> values;
    private final boolean valuesContainsStrings;

    public PinotJsonContainsPredicate(Call call)
    {
        List<ConnectorExpression> containsCallArgs = call.getArguments();
        Constant arrayArg = (Constant) containsCallArgs.getFirst();
        values = new ArrayList<>();
        if (arrayArg.getValue() instanceof VariableWidthBlock stringArray) {
            valuesContainsStrings = true;
            for (int index = 0; index < stringArray.getPositionCount(); index++) {
                values.add(stringArray.getSlice(index).toStringUtf8());
            }
        }
        else if (arrayArg.getValue() instanceof IntArrayBlock intArray) {
            valuesContainsStrings = false;
            for (int index = 0; index < intArray.getPositionCount(); index++) {
                values.add(String.valueOf(intArray.getInt(index)));
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported array argument type: " + arrayArg.getValue());
        }

        Call innerCall = (Call) containsCallArgs.get(1);
        Call jsonExtractScalarCall = innerCall;
        if (CAST.equals(innerCall.getFunctionName())) {
            jsonExtractScalarCall = (Call) innerCall.getArguments().getFirst();
        }

        List<ConnectorExpression> args = jsonExtractScalarCall.getArguments();
        columnName = ((Variable) args.get(0)).getName();
        jsonPath = ((Slice) ((Constant) args.get(1)).getValue()).toStringUtf8();
    }

    @Override
    public String toPql()
    {
        String escape = valuesContainsStrings ? "''" : "";
        String values = String.join(String.format("%s,%s", escape, escape), this.values);
        return String.format("JSON_MATCH(%s, '\"%s\" IN (%s%s%s)')",
                columnName, jsonPath, escape, values, escape);
    }

    public static boolean supportsCall(Call call)
    {
        if (!CONTAINS.equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();
        ConnectorExpression arrayArg = arguments.get(0);
        if (!(arrayArg instanceof Constant) || !(arguments.get(1) instanceof Call innerCall)) {
            return false;
        }

        Constant constant = (Constant) arrayArg;
        if (!(constant.getValue() instanceof VariableWidthBlock || constant.getValue() instanceof IntArrayBlock)) {
            return false;
        }

        if (CAST.equals(innerCall.getFunctionName())) {
            List<ConnectorExpression> castArguments = innerCall.getArguments();
            if (!(castArguments.getFirst() instanceof Call jsonExtracatScalarCall)) {
                return false;
            }
            return PinotPredicate.isSupportedCall(jsonExtracatScalarCall, "json_extract_scalar");
        }
        return PinotPredicate.isSupportedCall(innerCall, "json_extract_scalar");
    }
}
