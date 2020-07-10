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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.Util.domainsMatch;
import static java.util.Objects.requireNonNull;

public class ConnectorAwareTableScanMatcher
        implements Matcher
{
    private final Predicate<ConnectorTableHandle> expectedTable;
    private final TupleDomain<Predicate<ColumnHandle>> expectedEnforcedConstraint;

    public ConnectorAwareTableScanMatcher(Predicate<ConnectorTableHandle> expectedTable, TupleDomain<Predicate<ColumnHandle>> expectedEnforcedConstraint)
    {
        this.expectedTable = requireNonNull(expectedTable, "expectedTable is null");
        this.expectedEnforcedConstraint = requireNonNull(expectedEnforcedConstraint, "expectedEnforcedConstraint is null");
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

        return new MatchResult(tableMatches && domainsMatch(expectedEnforcedConstraint, actual));
    }

    public static PlanMatchPattern create(Predicate<ConnectorTableHandle> table, TupleDomain<Predicate<ColumnHandle>> constraints)
    {
        return node(TableScanNode.class)
                .with(new ConnectorAwareTableScanMatcher(table, constraints));
    }
}
