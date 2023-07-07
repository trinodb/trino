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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule;

public class MongoExpressionRewriter
{
    private final ImmutableSet.Builder<ConnectorExpressionRule<?, FilterExpression>> rules = ImmutableSet.builder();

    public MongoExpressionRewriter()
    {
        rules.add(new RewriteVariable());
        rules.add(new RewriteVarcharConstant());
        rules.add(new RewriteExactNumericConstant());
        rules.add(new RewriteAnd());
        rules.add(new RewriteOr());
        rules.add(new RewriteComparison());
        rules.add(new RewriteIn());
        rules.add(new RewriteIsNull());
        rules.add(new RewriteNot());
    }

    public ConnectorExpressionRewriter<FilterExpression> getRewriter()
    {
        return new ConnectorExpressionRewriter<>(rules.build());
    }
}
