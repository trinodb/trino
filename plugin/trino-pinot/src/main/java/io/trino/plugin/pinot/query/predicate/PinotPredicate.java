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

import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;

import java.util.List;

public interface PinotPredicate
{
    static boolean supportsCall(Call call)
    {
        return false;
    }

    static boolean isSupportedCall(Call call, String functionName)
    {
        if (!new FunctionName(functionName).equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (arguments.size() != 2) {
            return false;
        }

        if (!(arguments.get(0) instanceof Variable) || !(arguments.get(1) instanceof Constant)) {
            return false;
        }

        // TODO: resolve dependency issues to allow usage of io.trino.type.JsonType
        // return arguments.get(0).getType() instanceof JsonType;

        return true;
    }

    String toPql();
}
