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
package io.trino.sql.ir.optimizer.rule;

import io.trino.Session;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Evaluates a constant Bind expression
 */
public class EvaluateBind
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> contextBindings)
    {
        if (!(expression instanceof Bind bind)) {
            return Optional.empty();
        }

        if (bind.values().stream().noneMatch(Constant.class::isInstance)) {
            return Optional.empty();
        }

        Map<String, Constant> bindings = new HashMap<>();

        List<Symbol> newArguments = new ArrayList<>();
        List<Expression> newBindings = new ArrayList<>();
        for (int i = 0; i < bind.values().size(); i++) {
            Symbol argument = bind.function().arguments().get(i);
            Expression value = bind.values().get(i);

            if (value instanceof Constant constant) {
                bindings.put(argument.name(), constant);
            }
            else {
                newArguments.add(argument);
                newBindings.add(value);
            }
        }
        for (int i = bind.values().size(); i < bind.function().arguments().size(); i++) {
            newArguments.add(bind.function().arguments().get(i));
        }

        Optional<Expression> body = substituteBindings(bind.function().body(), bindings);
        if (body.isEmpty()) {
            return Optional.empty();
        }

        if (newBindings.isEmpty()) {
            return Optional.of(new Lambda(newArguments, body.orElseThrow()));
        }

        return Optional.of(new Bind(newBindings, new Lambda(newArguments, body.orElseThrow())));
    }

    private Optional<Expression> substituteBindings(Expression expression, Map<String, Constant> bindings)
    {
        ExpressionTreeRewriter<Void> rewriter = new ExpressionTreeRewriter<>(
                new ExpressionRewriter<>()
                {
                    @Override
                    public Expression rewriteReference(Reference reference, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        Constant constant = bindings.get(reference.name());
                        return constant == null ? reference : constant;
                    }
                });

        Expression rewritten = rewriter.rewrite(expression, null);
        return rewritten != expression ? Optional.of(rewritten) : Optional.empty();
    }
}
