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

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.sanity.PlanSanityChecker;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;

import java.util.List;

import static io.prestosql.spi.connector.StandardWarningCode.DREPRCATED_FUNCTION;

public final class DeprecatedFunctionWarning
        implements PlanSanityChecker.Checker
{
    private List<String> deprecatedFunctions = ImmutableList.of("json_array_get");

    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        for (Expression expression : ExpressionExtractor.extractExpressions(plan)) {
            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitFunctionCall(FunctionCall node, Void context)
                {
                    if (node.getName().getParts().stream().anyMatch(deprecatedFunctions::contains)) {
                        warningCollector.add(new PrestoWarning(DREPRCATED_FUNCTION, "Detected use of deprecated function: json_array_get()"));
                    }
                    return null;
                }
            }.process(expression, null);
        }
    }
}
