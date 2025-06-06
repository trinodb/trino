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
package io.trino.plugin.prometheus.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.arguments;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.prometheus.expression.RewriteHelpers.LABEL_MAP_CHECK_CORRECT_TYPE;
import static io.trino.plugin.prometheus.expression.RewriteHelpers.extractLabelName;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteIn
        implements ConnectorExpressionRule<Call, List<LabelFilterExpression>>
{
    private static final Capture<Call> LABEL_NAME = newCapture();
    private static final Capture<List<ConnectorExpression>> LABEL_VALUES = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(IN_PREDICATE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(call()
                    .matching(LABEL_MAP_CHECK_CORRECT_TYPE)
                    .capturedAs(LABEL_NAME)))
            .with(argument(1).matching(call()
                    .with(functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME))
                    .matching(call -> call.getArguments().stream().map(ConnectorExpression::getType).allMatch(RewriteHelpers::isCharOrStringType))
                    .with(arguments().capturedAs(LABEL_VALUES))));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<List<LabelFilterExpression>> rewrite(Call call, Captures captures, RewriteContext<List<LabelFilterExpression>> context)
    {
        String labelName = extractLabelName(captures, LABEL_NAME);
        List<ConnectorExpression> expressions = captures.get(LABEL_VALUES);

        return Optional.of(ImmutableList.of(new LabelFilterExpression(
                labelName,
                expressions.stream()
                        .map(RewriteHelpers::extractVarchar)
                        .distinct()
                        .collect(Collectors.joining("|", "(", ")")),
                true,
                false)));
    }
}
