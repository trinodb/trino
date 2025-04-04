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
import io.trino.spi.expression.FunctionName;

import java.util.List;

public class PinotNotJsonContainsPredicate
        implements PinotPredicate
{
    private static final FunctionName NOT = new FunctionName("$not");
    private final String pql;

    public PinotNotJsonContainsPredicate(Call call)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        pql = new PinotJsonContainsPredicate((Call) arguments.getFirst())
                .toPql()
                .replace("IN (", "NOT IN (");
    }

    @Override
    public String toPql()
    {
        return pql;
    }

    public static boolean supportsCall(Call call)
    {
        if (!NOT.equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();

        return PinotJsonContainsPredicate.supportsCall((Call) arguments.getFirst());
    }
}
