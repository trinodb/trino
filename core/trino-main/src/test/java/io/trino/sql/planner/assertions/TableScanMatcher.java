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
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static java.util.Objects.requireNonNull;

public final class TableScanMatcher
        implements Matcher
{
    private final String expectedTableName;
    private final Optional<Map<String, Domain>> expectedConstraint;
    private final Optional<Boolean> hasTableLayout;

    public TableScanMatcher(String expectedTableName, Optional<Map<String, Domain>> expectedConstraint, Optional<Boolean> hasTableLayout)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        this.expectedConstraint = requireNonNull(expectedConstraint, "expectedConstraint is null");
        this.hasTableLayout = requireNonNull(hasTableLayout, "hasTableLayout is null");
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
        CatalogSchemaTableName tableName = metadata.getTableName(session, tableScanNode.getTable());
        String actualTableName = tableName.getSchemaTableName().getTableName();

        // TODO (https://github.com/trinodb/trino/issues/17) change to equals()
        if (!expectedTableName.equalsIgnoreCase(actualTableName)) {
            return NO_MATCH;
        }

        if (!domainsMatch(expectedConstraint, tableScanNode.getEnforcedConstraint(), tableScanNode.getTable(), session, metadata)) {
            return NO_MATCH;
        }

        return new MatchResult(true);
    }

    private static boolean domainsMatch(
            Optional<Map<String, Domain>> expectedDomains,
            TupleDomain<ColumnHandle> actualConstraint,
            TableHandle tableHandle,
            Session session,
            Metadata metadata)
    {
        if (expectedDomains.isEmpty()) {
            return true;
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        TupleDomain<ColumnHandle> expected = TupleDomain.withColumnDomains(expectedDomains.get())
                .transformKeys(columnHandles::get);

        return expected.equals(actualConstraint);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("expectedTableName", expectedTableName)
                .add("expectedConstraint", expectedConstraint.orElse(null))
                .add("hasTableLayout", hasTableLayout.orElse(null))
                .toString();
    }
}
