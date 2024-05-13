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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ArrayFieldDereference;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.SystemSessionProperties.isPushFieldDereferenceLambdaIntoScanEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.PartialTranslator.extractPartialTranslations;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getReferences;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Function.identity;

/**
 * This rule will try to retrieve field reference expressions within lambda function and generate subfields into table scan
 * The rule is purposely being very narrow for a few reasons:
 * 1. Waiting on decision to accept Subfield to replace list of dereference names that currently being used
 * 2. This serves as a starting point to push lambda expression into table scan, and push lambda expression through other operators in the future
 * 3. The PruneUnnestMappings has NOT been accepted yet, which this rule is relying on, there is risk that this need to be rewritten
 *
 * TODO: Remove lambda expression after subfields are pushed down
 */
public class PushFieldReferenceLambdaIntoTableScan
        implements Rule<ProjectNode>
{
    private static final Logger LOG = Logger.get(PushFieldReferenceLambdaIntoTableScan.class);
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final PlannerContext plannerContext;

    public PushFieldReferenceLambdaIntoTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = plannerContext;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session)
                && isPushFieldDereferenceLambdaIntoScanEnabled(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Session session = context.getSession();

        // Extract only ArrayFieldDereference expressions from projection expressions, other expressions have been applied
        Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = project.getAssignments().getMap().entrySet().stream()
                .flatMap(expression ->
                        extractPartialTranslations(
                                expression.getValue(),
                                session,
                                true
                        ).entrySet().stream().filter(entry -> (entry.getValue() instanceof ArrayFieldDereference)))
                // Avoid duplicates
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (first, ignore) -> first));

        if (partialTranslations.isEmpty()) {
            return Result.empty();
        }

        Map<String, Symbol> inputVariableMappings = tableScan.getAssignments().keySet().stream()
                .collect(toImmutableMap(Symbol::name, identity()));
        Map<String, ColumnHandle> assignments = inputVariableMappings.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> tableScan.getAssignments().get(entry.getValue())));

        // Because we will not replace any symbol references but prune the data, we want to make sure same table scan symbol
        // is not used anywhere else just to be safe, we will revisit this once we need to expand the scope of this optimization.
        // As a result, only support limited cases now which symbol reference has to be uniquely referenced
        List<Expression> expressions = ImmutableList.copyOf(project.getAssignments().getExpressions());
        Map<String, Long> referenceNamesCount = expressions.stream()
                .flatMap(expression -> getReferences(expression).stream())
                .map(Reference::name)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        partialTranslations = partialTranslations.entrySet().stream().filter(entry -> {
            ArrayFieldDereference arrayFieldDereference = (ArrayFieldDereference) entry.getValue();
            return arrayFieldDereference.getTarget() instanceof Variable variable
                    && referenceNamesCount.get(variable.getName()) == 1;
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (partialTranslations.isEmpty()) {
            return Result.empty();
        }

        // At this point, only Hive connector understands how to deal with ArrayFieldDereference expression
        Optional<ProjectionApplicationResult<TableHandle>> result =
                plannerContext.getMetadata().applyProjection(session,
                        tableScan.getTable(),
                        ImmutableList.copyOf(partialTranslations.values()),
                        assignments);

        if (result.isEmpty()) {
            return Result.empty();
        }

        Map<Symbol, ColumnHandle> newTableAssignments = new HashMap<>();
        for (Assignment assignment : result.get().getAssignments()) {
            newTableAssignments.put(inputVariableMappings.get(assignment.getVariable()), assignment.getColumn());
        }

        verify(assignments.size() == newTableAssignments.size(),
                "Assignments size mis-match after PushSubscriptLambdaIntoTableScan: %d instead of %d",
                newTableAssignments.size(),
                assignments.size());

        LOG.info("PushSubscriptLambdaIntoTableScan is effectively triggered on %d expressions", partialTranslations.size());

        // Only update tableHandle and TableScan assignments which have new columnHandles
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new TableScanNode(
                                tableScan.getId(),
                                result.get().getHandle(),
                                tableScan.getOutputSymbols(),
                                newTableAssignments,
                                tableScan.getEnforcedConstraint(),
                                tableScan.getStatistics(),
                                tableScan.isUpdateTarget(),
                                tableScan.getUseConnectorNodePartitioning()),
                        project.getAssignments()));
    }
}
