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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;

public class NotRewriteRule
        implements ConnectorExpressionRule<Call, Document>
{
    private static final Pattern<Call> PATTERN = call().with(functionName().equalTo(NOT_FUNCTION_NAME));

    public NotRewriteRule()
    {
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
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

        return Optional.of(new Document("$nor", childrenPredicates));
    }
}
