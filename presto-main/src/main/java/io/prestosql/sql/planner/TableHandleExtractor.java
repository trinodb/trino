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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.Optional;

public class TableHandleExtractor
{
    public Multimap<CatalogName, ConnectorTableHandle> extractTableHandles(PlanNode root)
    {
        Visitor visitor = new Visitor();
        root.accept(visitor, null);
        return visitor.getTableHandles();
    }

    private static class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final ImmutableMultimap.Builder<CatalogName, ConnectorTableHandle> tableHandles = ImmutableMultimap.builder();

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            tableHandles.put(node.getTable().getCatalogName(), node.getTable().getConnectorHandle());
            return visitPlan(node, context);
        }

        @Override
        public Void visitDelete(DeleteNode node, Void context)
        {
            TableHandle tableHandle = node.getTarget().getHandle();
            tableHandles.put(tableHandle.getCatalogName(), tableHandle.getConnectorHandle());
            return visitPlan(node, context);
        }

        @Override
        public Void visitTableDelete(TableDeleteNode node, Void context)
        {
            TableHandle tableHandle = node.getTarget();
            tableHandles.put(tableHandle.getCatalogName(), tableHandle.getConnectorHandle());
            return visitPlan(node, context);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            Optional<TableHandle> tableHandle = node.getTarget().getTableHandle();
            tableHandle.map(handle -> tableHandles.put(handle.getCatalogName(), handle.getConnectorHandle()));
            return visitPlan(node, context);
        }

        private Multimap<CatalogName, ConnectorTableHandle> getTableHandles()
        {
            return tableHandles.build();
        }
    }
}
