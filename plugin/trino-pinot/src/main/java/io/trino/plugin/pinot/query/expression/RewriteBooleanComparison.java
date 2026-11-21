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

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

/**
 * Implements {@code predicate = true}, {@code predicate = false}, {@code predicate != true} and {@code predicate != false}.
 */
public class RewriteBooleanComparison
        implements ConnectorExpressionRule<Call, String>
{
    private static final Capture<Call> PREDICATE = newCapture();
    private static final Capture<Constant> VALUE = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().matching(name -> name.equals(EQUAL_OPERATOR_FUNCTION_NAME) || name.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(call().with(type().equalTo(BOOLEAN)).capturedAs(PREDICATE)))
            .with(argument(1).matching(constant().with(type().equalTo(BOOLEAN)).capturedAs(VALUE)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        Object value = captures.get(VALUE).getValue();
        if (value == null) {
            return Optional.empty();
        }
        Optional<String> predicate = context.defaultRewrite(captures.get(PREDICATE));
        if (predicate.isEmpty()) {
            return Optional.empty();
        }
        boolean negate = call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME) != (boolean) value;
        return Optional.of(negate ? "NOT(%s)".formatted(predicate.get()) : predicate.get());
    }
}
