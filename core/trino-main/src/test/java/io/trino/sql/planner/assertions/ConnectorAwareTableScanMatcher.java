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
package io.trino.sql.planner.assertions;

import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.Util.domainsMatch;
import static java.util.Objects.requireNonNull;

public class ConnectorAwareTableScanMatcher
        implements Matcher
{
    private final Predicate<ConnectorTableHandle> expectedTable;
    private final TupleDomain<Predicate<ColumnHandle>> expectedEnforcedConstraint;
    private final Predicate<Optional<PlanNodeStatsEstimate>> expectedStatistics;

    public ConnectorAwareTableScanMatcher(
            Predicate<ConnectorTableHandle> expectedTable,
            TupleDomain<Predicate<ColumnHandle>> expectedEnforcedConstraint,
            Predicate<Optional<PlanNodeStatsEstimate>> expectedStatistics)
    {
        this.expectedTable = requireNonNull(expectedTable, "expectedTable is null");
        this.expectedEnforcedConstraint = requireNonNull(expectedEnforcedConstraint, "expectedEnforcedConstraint is null");
        this.expectedStatistics = requireNonNull(expectedStatistics, "expectedStatistics is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableScanNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableScanNode tableScanNode = (TableScanNode) node;

        TupleDomain<ColumnHandle> actual = tableScanNode.getEnforcedConstraint();

        boolean tableMatches = expectedTable.test(tableScanNode.getTable().getConnectorHandle());
        boolean domainsMatch = domainsMatch(expectedEnforcedConstraint, actual);
        boolean statisticMatch = expectedStatistics.test(tableScanNode.getStatistics());

        return new MatchResult(tableMatches && domainsMatch && statisticMatch);
    }

    public static PlanMatchPattern create(
            Predicate<ConnectorTableHandle> table,
            TupleDomain<Predicate<ColumnHandle>> constraints,
            Predicate<Optional<PlanNodeStatsEstimate>> expectedStatistics)
    {
        return node(TableScanNode.class)
                .with(new ConnectorAwareTableScanMatcher(table, constraints, expectedStatistics));
    }
}
