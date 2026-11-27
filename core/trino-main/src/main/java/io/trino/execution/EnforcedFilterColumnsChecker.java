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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.planner.SimplePlanVisitor;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.sql.util.AstUtils.preOrder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class EnforcedFilterColumnsChecker
{
    private final EnforcedFilterColumnsConfig config;

    public EnforcedFilterColumnsChecker(EnforcedFilterColumnsConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    public void validate(Analysis analysis, PlanNode plan, Session session, PlannerContext plannerContext)
    {
        if (config.getEnforcedFilterColumns().isEmpty()) {
            return;
        }

        Set<TableColumn> columnsReferencedInWhere = extractColumnsReferencedInWhere(analysis);
        plan.accept(new SimplePlanVisitor<Void>()
        {
            @Override
            public Void visitTableScan(TableScanNode node, Void context)
            {
                CatalogSchemaTableName tableName = plannerContext.getMetadata().getTableName(session, node.getTable());
                SchemaTableName schemaTableName = tableName.getSchemaTableName();
                String catalogName = tableName.getCatalogName();
                String schemaName = schemaTableName.getSchemaName();
                String table = schemaTableName.getTableName();

                Set<String> enforcedFilterColumns = config.getEnforcedFilterColumnsForTable(catalogName, schemaName, table);
                if (enforcedFilterColumns.isEmpty()) {
                    return null;
                }

                Map<String, ColumnHandle> columnHandlesByLowerCase = plannerContext.getMetadata().getColumnHandles(session, node.getTable()).entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> entry.getKey().toLowerCase(ENGLISH),
                                Map.Entry::getValue,
                                (first, _) -> first));
                Set<ColumnHandle> enforcedColumnHandles = node.getEnforcedConstraint().getDomains()
                        .map(Map::keySet)
                        .orElse(ImmutableSet.of());

                List<String> missingColumns = new ArrayList<>();
                for (String enforcedColumn : enforcedFilterColumns) {
                    String normalizedColumn = enforcedColumn.toLowerCase(ENGLISH);
                    ColumnHandle columnHandle = columnHandlesByLowerCase.get(normalizedColumn);
                    if (columnHandle == null) {
                        // The configured column does not exist in this table, so it cannot be required for this scan.
                        continue;
                    }

                    boolean referencedInWhere = columnsReferencedInWhere.contains(new TableColumn(catalogName, schemaName, table, normalizedColumn));
                    if (!referencedInWhere && !enforcedColumnHandles.contains(columnHandle)) {
                        missingColumns.add(format("%s.%s.%s.%s", catalogName, schemaName, table, enforcedColumn));
                    }
                }

                if (!missingColumns.isEmpty()) {
                    throw new TrinoException(
                            PERMISSION_DENIED,
                            format(
                                    "Enforced columns %s must be referenced in the WHERE clause when querying table %s",
                                    missingColumns,
                                    tableName));
                }
                return null;
            }
        }, null);
    }

    private static Set<TableColumn> extractColumnsReferencedInWhere(Analysis analysis)
    {
        Statement statement = analysis.getStatement();
        if (statement == null) {
            return ImmutableSet.of();
        }

        Map<NodeRef<Expression>, ResolvedField> columnReferences = analysis.getColumnReferenceFields();
        ImmutableSet.Builder<TableColumn> columns = ImmutableSet.builder();
        preOrder(statement)
                .filter(QuerySpecification.class::isInstance)
                .map(QuerySpecification.class::cast)
                .forEach(querySpecification -> {
                    Expression where = analysis.getWhere(querySpecification);
                    if (where == null) {
                        return;
                    }

                    preOrder(where)
                            .filter(Expression.class::isInstance)
                            .map(Expression.class::cast)
                            .forEach(expression -> {
                                ResolvedField resolvedField = columnReferences.get(NodeRef.of(expression));
                                if (resolvedField == null) {
                                    return;
                                }
                                Field field = resolvedField.getField();
                                field.getOriginTable().ifPresent(table -> field.getOriginColumnName().ifPresent(column ->
                                        columns.add(new TableColumn(table.catalogName(), table.schemaName(), table.objectName(), column))));
                            });
                });
        return columns.build();
    }

    private record TableColumn(String catalogName, String schemaName, String tableName, String columnName)
    {
        private TableColumn
        {
            catalogName = catalogName.toLowerCase(ENGLISH);
            schemaName = schemaName.toLowerCase(ENGLISH);
            tableName = tableName.toLowerCase(ENGLISH);
            columnName = columnName.toLowerCase(ENGLISH);
        }
    }
}
