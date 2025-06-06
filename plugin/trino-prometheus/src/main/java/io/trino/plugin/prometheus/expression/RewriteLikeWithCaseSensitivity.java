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
import io.trino.spi.expression.Constant;

import java.util.List;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.prometheus.expression.RewriteHelpers.LABEL_MAP_CHECK_CORRECT_TYPE;
import static io.trino.plugin.prometheus.expression.RewriteHelpers.extractLabelName;
import static io.trino.plugin.prometheus.expression.RewriteHelpers.extractVarchar;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteLikeWithCaseSensitivity
        implements ConnectorExpressionRule<Call, List<LabelFilterExpression>>
{
    private static final Capture<Call> LABEL_NAME = newCapture();
    private static final Capture<Constant> LABEL_VALUE_LIKE_PATTERN = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(LIKE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(call()
                    .matching(LABEL_MAP_CHECK_CORRECT_TYPE)
                    .capturedAs(LABEL_NAME)))
            .with(argument(1).matching(constant()
                    .with(type().matching(RewriteHelpers::isCharOrStringType))
                    .capturedAs(LABEL_VALUE_LIKE_PATTERN)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<List<LabelFilterExpression>> rewrite(Call expression, Captures captures, RewriteContext<List<LabelFilterExpression>> context)
    {
        String labelName = extractLabelName(captures, LABEL_NAME);
        String labelValue = extractVarchar(captures.get(LABEL_VALUE_LIKE_PATTERN));

        return Optional.of(ImmutableList.of(new LabelFilterExpression(
                labelName,
                labelValue.replaceAll("%", ".*").replaceAll("_", "."),
                true,
                false)));
    }
}
