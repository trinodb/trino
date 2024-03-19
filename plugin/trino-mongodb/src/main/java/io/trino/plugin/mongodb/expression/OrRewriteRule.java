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
import io.trino.matching.Property;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OrRewriteRule
        implements ConnectorExpressionRule<Call, Document>
{
    private final Pattern<Call> expressionPattern;

    public OrRewriteRule()
    {
        Pattern<Call> p = Pattern.typeOf(Call.class);
        Property<Call, ?, FunctionName> functionName = Property.property("functionName", Call::getFunctionName);
        this.expressionPattern = p.with(functionName.equalTo(new FunctionName("$or")));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return (Pattern<Call>) this.expressionPattern;
    }

    @Override
    public Optional<Document> rewrite(Call expression, Captures captures, RewriteContext<Document> context)
    {
        List<Document> childrenPredicates = new ArrayList<>();

        for (ConnectorExpression exp : expression.getArguments()) {
            Optional<Document> predicate = context.defaultRewrite(exp);
            if (predicate.isPresent()) {
                childrenPredicates.add(predicate.get());
            }
            else {
                return Optional.empty();
            }
        }

        return Optional.of(new Document("$or", childrenPredicates));
    }
}
