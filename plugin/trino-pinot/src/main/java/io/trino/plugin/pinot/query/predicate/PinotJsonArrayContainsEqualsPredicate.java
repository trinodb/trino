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
import io.trino.spi.type.BooleanType;

import java.util.List;

public class PinotJsonArrayContainsEqualsPredicate
        implements PinotPredicate
{
    private String pql;

    public static boolean supportsCall(Call call)
    {
        if (!new FunctionName("$equal").equals(call.getFunctionName()) &&
                !new FunctionName("$not_equal").equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();

        if (!PinotJsonArrayContainsPredicate.supportsCall((Call) arguments.getFirst())) {
            return false;
        }

        return arguments.get(1) instanceof Constant && arguments.get(1).getType() instanceof BooleanType;
    }

    public PinotJsonArrayContainsEqualsPredicate(Call call)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        boolean negate = false;

        boolean booleanValue = (boolean) ((Constant) arguments.get(1)).getValue();
        if (new FunctionName("$equal").equals(call.getFunctionName()) && !booleanValue) {
            negate = true;
        }
        else if (new FunctionName("$not_equal").equals(call.getFunctionName()) && booleanValue) {
            negate = true;
        }

        pql = new PinotJsonArrayContainsPredicate((Call) arguments.getFirst()).toPQL();
        if (negate) {
            pql = "NOT(" + pql + ")";
        }
    }

    @Override
    public String toPQL()
    {
        return pql;
    }
}
