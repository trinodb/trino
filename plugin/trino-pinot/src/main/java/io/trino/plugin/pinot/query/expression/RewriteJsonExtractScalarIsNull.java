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
package io.trino.plugin.pinot.query.expression;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.columnReference;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.jsonMatch;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.jsonPath;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.pathOf;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;

/**
 * Implements {@code json_extract_scalar(column, path) IS NULL}.
 */
public class RewriteJsonExtractScalarIsNull
        implements ConnectorExpressionRule<Call, String>
{
    static final FunctionName JSON_EXTRACT_SCALAR = new FunctionName("json_extract_scalar");

    private static final Capture<Variable> COLUMN = newCapture();
    private static final Capture<Constant> PATH = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(IS_NULL_FUNCTION_NAME))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(call()
                    .with(functionName().equalTo(JSON_EXTRACT_SCALAR))
                    .with(argumentCount().equalTo(2))
                    .with(argument(0).matching(variable().capturedAs(COLUMN)))
                    .with(argument(1).matching(constant().capturedAs(PATH)))));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        Optional<String> column = columnReference(context, captures.get(COLUMN));
        Optional<String> path = pathOf(captures.get(PATH));
        if (column.isEmpty() || path.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jsonMatch(column.get(), jsonPath(path.get()) + " IS NULL"));
    }
}
