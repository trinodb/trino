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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.DateType;

import java.time.LocalDate;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.toDate;

public class RewriteDateConstant
        extends RewriteConstant
        implements ConnectorExpressionRule<Constant, MongoExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(DateType.class::isInstance));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<MongoExpression> rewrite(Constant constant, Captures captures, RewriteContext<MongoExpression> context)
    {
        Object value = constant.getValue();
        if (value == null) {
            return handleNullValue();
        }

        long days = (long) value;
        return Optional.of(new MongoExpression(toDate(LocalDate.ofEpochDay(days).toString())));
    }
}
