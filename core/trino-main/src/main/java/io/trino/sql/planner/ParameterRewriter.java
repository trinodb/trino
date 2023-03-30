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
package io.trino.sql.planner;

import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.util.Objects.requireNonNull;

public class ParameterRewriter
        extends ExpressionRewriter<Void>
{
    private final Map<NodeRef<Parameter>, Expression> parameters;
    private final Analysis analysis;

    public ParameterRewriter(Map<NodeRef<Parameter>, Expression> parameters)
    {
        requireNonNull(parameters, "parameters is null");
        this.parameters = parameters;
        this.analysis = null;
    }

    public ParameterRewriter(Analysis analysis)
    {
        this.analysis = analysis;
        this.parameters = analysis.getParameters();
    }

    @Override
    protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return treeRewriter.defaultRewrite(node, context);
    }

    @Override
    public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        checkState(parameters.size() > node.getId(), "Too few parameter values");
        return coerceIfNecessary(node, parameters.get(NodeRef.of(node)));
    }

    private Expression coerceIfNecessary(Expression original, Expression rewritten)
    {
        if (analysis == null) {
            return rewritten;
        }

        Type coercion = analysis.getCoercion(original);
        if (coercion != null) {
            rewritten = new Cast(
                    rewritten,
                    toSqlType(coercion),
                    false,
                    analysis.isTypeOnlyCoercion(original));
        }
        return rewritten;
    }
}
