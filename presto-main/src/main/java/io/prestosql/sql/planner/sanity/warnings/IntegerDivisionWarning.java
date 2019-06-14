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
package io.prestosql.sql.planner.sanity.warnings;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.sanity.PlanSanityChecker;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.Expression;

import static io.prestosql.spi.connector.StandardWarningCode.INTEGER_DIVISION;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;

public final class IntegerDivisionWarning
        implements PlanSanityChecker.Checker
{
    public IntegerDivisionWarning() {}

    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        for (Expression expression : ExpressionExtractor.extractExpressions(plan)) {
            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
                {
                    if (node.getOperator().equals(DIVIDE)) {
                        Type leftType = typeAnalyzer.getType(session, types, node.getLeft());
                        Type rightType = typeAnalyzer.getType(session, types, node.getRight());
                        if (leftType.equals(INTEGER) && rightType.equals(INTEGER)) {
                            warningCollector.add(new PrestoWarning(INTEGER_DIVISION, "Detected integer division. Possible loss of precision."));
                        }
                    }
                    return null;
                }
            }.process(expression, null);
        }
    }
}
