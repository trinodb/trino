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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.toDate;

public class RewriteFromIso8601Conversion
        implements ConnectorExpressionRule<Call, MongoExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();

    private final Pattern<Call> pattern;

    public RewriteFromIso8601Conversion(FunctionName functionName, Predicate<? super Type> typeMatchingPredicate)
    {
        this.pattern = call()
                .with(functionName().equalTo(functionName))
                .with(type().matching(typeMatchingPredicate))
                .with(argumentCount().equalTo(1))
                .with(argument(0).matching(expression().capturedAs(VALUE)));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<MongoExpression> rewrite(Call call, Captures captures, RewriteContext<MongoExpression> context)
    {
        Optional<MongoExpression> value = context.defaultRewrite(captures.get(VALUE));
        return value.map(mongoExpression -> new MongoExpression(toDate(mongoExpression.expression())));
    }
}
