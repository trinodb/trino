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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class ConnectorTableCredentialsVisitor
        extends SimplePlanVisitor<Void>
{
    private final Metadata metadata;
    private final Session session;
    private final ImmutableMap.Builder<PlanNodeId, ConnectorTableCredentials> builder;

    public ConnectorTableCredentialsVisitor(Session session, Metadata metadata, ImmutableMap.Builder<PlanNodeId, ConnectorTableCredentials> builder)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.builder = requireNonNull(builder, "builder is null");
    }

    @Override
    public Void visitMergeWriter(MergeWriterNode node, Void context)
    {
        TableWriterNode.MergeTarget target = node.getTarget();
        extract(builder, node, metadata.getTableCredentials(session, target.getHandle().catalogHandle(), target.getHandle().connectorHandle()));
        return super.visitMergeWriter(node, context);
    }

    @Override
    public Void visitTableExecute(TableExecuteNode node, Void context)
    {
        TableWriterNode.TableExecuteTarget target = node.getTarget();
        extract(builder, node, metadata.getTableCredentials(session, target.getExecuteHandle().catalogHandle(), target.getExecuteHandle().connectorHandle()));
        return super.visitTableExecute(node, context);
    }

    @Override
    public Void visitTableWriter(TableWriterNode node, Void context)
    {
        switch (node.getTarget()) {
            case TableWriterNode.MergeTarget mergeTarget -> extract(builder, node, metadata.getTableCredentials(session, mergeTarget.getHandle().catalogHandle(), mergeTarget.getHandle().connectorHandle()));
            case TableWriterNode.RefreshMaterializedViewTarget materializedViewTarget -> extract(builder, node, metadata.getTableCredentials(session, materializedViewTarget.getTableHandle().catalogHandle(), materializedViewTarget.getTableHandle().connectorHandle()));
            case TableWriterNode.TableExecuteTarget tableExecuteTarget -> extract(builder, node, metadata.getTableCredentials(session, tableExecuteTarget.getExecuteHandle().catalogHandle(), tableExecuteTarget.getExecuteHandle().connectorHandle()));
            case TableWriterNode.CreateTarget createTarget -> extract(builder, node, metadata.getTableCredentials(session, createTarget.getHandle().catalogHandle(), createTarget.getHandle().connectorHandle()));
            case TableWriterNode.InsertTarget insertTarget -> extract(builder, node, metadata.getTableCredentials(session, insertTarget.getHandle().catalogHandle(), insertTarget.getHandle().connectorHandle()));
            default -> throw new IllegalArgumentException("Unsupported table writer node: " + node.getClass().getSimpleName());
        }
        return super.visitTableWriter(node, context);
    }

    @Override
    public Void visitTableScan(TableScanNode node, Void context)
    {
        TableHandle table = node.getTable();
        extract(builder, node, metadata.getTableCredentials(session, table.catalogHandle(), table.connectorHandle()));
        return super.visitTableScan(node, context);
    }

    @Override
    public Void visitTableFunctionProcessor(TableFunctionProcessorNode node, Void context)
    {
        TableFunctionHandle handle = node.getHandle();
        extract(builder, node, metadata.getTableCredentials(session, handle.catalogHandle(), handle.functionHandle()));
        return super.visitTableFunctionProcessor(node, context);
    }

    private static void extract(ImmutableMap.Builder<PlanNodeId, ConnectorTableCredentials> builder, PlanNode node, Optional<ConnectorTableCredentials> credentials)
    {
        credentials.ifPresent(connectorTableCredentials -> builder.put(node.getId(), connectorTableCredentials));
    }
}
