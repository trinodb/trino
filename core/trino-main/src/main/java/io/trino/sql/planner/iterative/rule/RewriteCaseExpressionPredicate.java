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

package io.trino.sql.planner.iterative.rule;

import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.CaseExpressionPredicateRewriter;
import io.trino.sql.planner.TypeAnalyzer;

import static io.trino.SystemSessionProperties.isOptimizeCaseExpressionPredicate;

/**
 * This Rule rewrites a CASE expression predicate into a series of AND/OR clauses.
 * The following CASE expression
 * <p>
 * (CASE
 * WHEN expression=constant1 THEN result1
 * WHEN expression=constant2 THEN result2
 * WHEN expression=constant3 THEN result3
 * ELSE elseResult
 * END) = value
 * <p>
 * can be converted into a series AND/OR clauses as below
 * <p>
 * (result1 = value AND expression IS NOT NULL AND expression=constant1) OR
 * (result2 = value AND expression IS NOT NULL AND expression=constant2 AND !(expression=constant1)) OR
 * (result3 = value AND expression IS NOT NULL AND expression=constant3 AND !(expression=constant1) AND !(expression=constant2)) OR
 * (elseResult = value AND ((expression IS NULL) OR (!(expression=constant1) AND !(expression=constant2) AND !(expression=constant3))))
 * <p>
 * The above conversion evaluates the conditions in WHEN clauses multiple times. But if we ensure these conditions are
 * disjunct, we can skip all the NOT of previous WHEN conditions and simplify the expression to:
 * <p>
 * (result1 = value AND expression IS NOT NULL AND expression=constant1) OR
 * (result2 = value AND expression IS NOT NULL AND expression=constant2) OR
 * (result3 = value AND expression IS NOT NULL AND expression=constant3) OR
 * (elseResult = value AND ((expression IS NULL) OR (!(expression=constant1) AND !(expression=constant2) AND !(expression=constant3)))
 * <p>
 * To ensure the WHEN conditions are disjunct, the following criteria needs to be met:
 * 1. Value is either a constant or column reference or input reference and not any function
 * 2. The LHS expression in all WHEN clauses are the same and deterministic
 * For example, if one WHEN clause has a expression using col1 and another using col2, it will not work
 * 3. The relational operator in the WHEN clause is equals. With other operators it is hard to check for exclusivity.
 * 4. All the RHS expressions are a constant, non-null and unique
 * <p>
 * This conversion is done so that it is easy for the ExpressionInterpreter & other Optimizers to further
 * simplify this and construct a domain for the column that can be used by Readers .
 * i.e, ExpressionInterpreter can discard all conditions in which result != value and
 * RowExpressionDomainTranslator can construct a Domain for the column
 */
public class RewriteCaseExpressionPredicate
        extends ExpressionRewriteRuleSet
{
    public RewriteCaseExpressionPredicate(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        super((expression, context) -> CaseExpressionPredicateRewriter.rewrite(expression, context, plannerContext, typeAnalyzer));
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isOptimizeCaseExpressionPredicate(session);
    }
}
