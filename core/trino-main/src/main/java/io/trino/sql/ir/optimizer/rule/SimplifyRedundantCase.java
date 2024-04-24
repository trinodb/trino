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
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;

public class SimplifyRedundantCase
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public SimplifyRedundantCase(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case caseTerm)) {
            return Optional.empty();
        }

        // TODO: generalize to arbitrary number of clauses
        if (caseTerm.whenClauses().size() != 1) {
            return Optional.empty();
        }

        WhenClause thenClause = caseTerm.whenClauses().getFirst();
        if (thenClause.getResult().equals(TRUE) && caseTerm.defaultValue().equals(FALSE)) {
            return Optional.of(thenClause.getOperand());
        }

        if (thenClause.getResult().equals(FALSE) && caseTerm.defaultValue().equals(TRUE)) {
            return Optional.of(IrExpressions.not(metadata, thenClause.getOperand()));
        }

        return Optional.empty();
    }
}
