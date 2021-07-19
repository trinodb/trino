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
package io.trino.util;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.Map;

public class RowExpressionConverter
{
    private final Map<Symbol, Integer> sourceLayout;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final Metadata metadata;
    private final Session session;

    public RowExpressionConverter(Map<Symbol, Integer> sourceLayout, Map<NodeRef<Expression>,
            Type> expressionTypes, Metadata metadata, Session session)
    {
        this.sourceLayout = sourceLayout;
        this.expressionTypes = expressionTypes;
        this.metadata = metadata;
        this.session = session;
    }

    public RowExpression toRowExpression(Expression expression)
    {
        return SqlToRowExpressionTranslator.translateDynamicFilterExpression(expression, expressionTypes,
                sourceLayout, metadata, session, true);
    }
}
