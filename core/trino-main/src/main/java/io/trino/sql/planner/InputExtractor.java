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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.Column;
import io.trino.execution.Input;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InputExtractor
{
    private final Metadata metadata;
    private final Session session;

    public InputExtractor(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public List<Input> extractInputs(SubPlan root)
    {
        Visitor visitor = new Visitor();
        root.getAllFragments()
                .forEach(fragment -> fragment.getRoot().accept(visitor, fragment.getId()));

        return ImmutableList.copyOf(visitor.getInputs());
    }

    private static Column createColumn(ColumnMetadata columnMetadata)
    {
        return new Column(columnMetadata.getName(), columnMetadata.getType().toString());
    }

    private Input createInput(Session session, TableHandle table, Set<Column> columns, PlanFragmentId fragmentId, PlanNodeId planNodeId)
    {
        CatalogSchemaTableName tableName = metadata.getTableName(session, table);
        SchemaTableName schemaTable = tableName.getSchemaTableName();
        Optional<Object> inputMetadata = metadata.getInfo(session, table);
        return new Input(
                tableName.getCatalogName(),
                table.getCatalogHandle().getVersion(),
                schemaTable.getSchemaName(),
                schemaTable.getTableName(),
                inputMetadata,
                ImmutableList.copyOf(columns),
                fragmentId,
                planNodeId);
    }

    private class Visitor
            extends PlanVisitor<Void, PlanFragmentId>
    {
        private final ImmutableSet.Builder<Input> inputs = ImmutableSet.builder();

        public Set<Input> getInputs()
        {
            return inputs.build();
        }

        @Override
        public Void visitTableScan(TableScanNode node, PlanFragmentId fragmentId)
        {
            processScan(fragmentId, node.getId(), node.getTable(), node.getAssignments());
            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, PlanFragmentId fragmentId)
        {
            processScan(fragmentId, node.getId(), node.getTableHandle(), node.getAssignments());
            return null;
        }

        private void processScan(PlanFragmentId fragmentId, PlanNodeId planNodeId, TableHandle tableHandle, Map<Symbol, ColumnHandle> assignments)
        {
            Set<Column> columns = new HashSet<>();
            for (ColumnHandle columnHandle : assignments.values()) {
                columns.add(createColumn(metadata.getColumnMetadata(session, tableHandle, columnHandle)));
            }

            inputs.add(createInput(session, tableHandle, columns, fragmentId, planNodeId));
        }

        @Override
        protected Void visitPlan(PlanNode node, PlanFragmentId fragmentId)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, fragmentId);
            }
            return null;
        }
    }
}
