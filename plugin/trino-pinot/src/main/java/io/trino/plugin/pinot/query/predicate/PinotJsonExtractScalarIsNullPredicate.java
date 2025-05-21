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

import java.util.List;

public class PinotJsonExtractScalarIsNullPredicate
        implements PinotPredicate
{
    private static final FunctionName IS_NULL = new FunctionName("$is_null");
    private final String columnName;
    private final String jsonPath;

    public PinotJsonExtractScalarIsNullPredicate(Call call)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        Call innerCall = (Call) arguments.getFirst();
        List<ConnectorExpression> innerArgs = innerCall.getArguments();
        columnName = ((Variable) innerArgs.getFirst()).getName();
        jsonPath = ((Slice) ((Constant) innerArgs.get(1)).getValue()).toStringUtf8();
    }

    @Override
    public String toPql()
    {
        return String.format("JSON_MATCH(%s, '\"%s\" IS NULL')",
                columnName, jsonPath);
    }

    public static boolean supportsCall(Call call)
    {
        if (!IS_NULL.equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (!(arguments.getFirst() instanceof Call innerCall)) {
            return false;
        }

        if (!PinotPredicate.isSupportedCall(innerCall, "json_extract_scalar")) {
            return false;
        }

        return true;
    }
}
