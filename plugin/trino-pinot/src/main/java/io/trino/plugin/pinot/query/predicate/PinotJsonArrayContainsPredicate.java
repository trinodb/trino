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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.List;

public class PinotJsonArrayContainsPredicate
        implements PinotPredicate
{
    public static final FunctionName JSON_ARRAY_CONTAINS = new FunctionName("json_array_contains");
    private final String columnName;
    private final String jsonPath;
    private final String value;
    private final boolean valueIsString;

    public PinotJsonArrayContainsPredicate(Call call)
    {
        List<ConnectorExpression> jsonArrayContainsCallArgs = call.getArguments();
        Call jsonExtractCall = (Call) jsonArrayContainsCallArgs.get(0);
        List<ConnectorExpression> args = jsonExtractCall.getArguments();
        columnName = ((Variable) args.get(0)).getName();
        jsonPath = ((Slice) ((Constant) args.get(1)).getValue()).toStringUtf8();

        Constant constant = (Constant) jsonArrayContainsCallArgs.get(1);
        if (constant.getType() instanceof VarcharType) {
            valueIsString = true;
            value = ((Slice) constant.getValue()).toStringUtf8();
        }
        else {
            valueIsString = false;
            value = constant.getValue().toString();
        }
    }

    @Override
    public String toPql()
    {
        String quote = valueIsString ? "''" : "";
        return String.format("JSON_MATCH(%s, '\"%s[*]\" = %s%s%s')",
                columnName, jsonPath, quote, value, quote);
    }

    public static boolean supportsCall(Call call)
    {
        if (!JSON_ARRAY_CONTAINS.equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();

        if (arguments.size() != 2) {
            return false;
        }

        if (!(arguments.get(0) instanceof Call || arguments.get(1) instanceof Constant)) {
            return false;
        }

        return PinotPredicate.isSupportedCall((Call) arguments.get(0), "json_extract");
    }
}
