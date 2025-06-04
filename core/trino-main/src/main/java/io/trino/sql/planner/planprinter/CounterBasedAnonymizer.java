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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.TableInfo;
import io.trino.metadata.IndexHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionFormatter;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SystemPartitioningHandle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.Partitioning.ArgumentBinding;
import static io.trino.sql.planner.plan.StatisticsWriterNode.WriteStatisticsHandle;
import static io.trino.sql.planner.plan.StatisticsWriterNode.WriteStatisticsTarget;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import static io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewTarget;
import static io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * An implementation of {@link Anonymizer} which uses simple counter-based mechanism
 */
public class CounterBasedAnonymizer
        implements Anonymizer
{
    private enum ObjectType
    {
        CATALOG,
        SCHEMA,
        TABLE,
        COLUMN,
        SYMBOL,
        LITERAL,
        VALUE
    }

    private final ExpressionFormatter.Formatter anonymizeExpressionFormatter =
            new ExpressionFormatter.Formatter(Optional.of(this::anonymizeLiteral), Optional.of(this::anonymizeSymbolReference));
    private final Map<String, String> anonymizedMap = new HashMap<>();
    private final Map<ObjectType, Integer> counterMap = Arrays.stream(ObjectType.values())
            .collect(toMap(objectType -> objectType, objectType -> 0));

    @Override
    public String anonymize(Type type, String value)
    {
        String formattedTypeName = type.getDisplayName()
                .replaceAll("\\W", "_")
                .replaceAll("__", "_");
        return formattedTypeName + "_" + anonymize(value, ObjectType.VALUE);
    }

    @Override
    public String anonymize(Symbol symbol)
    {
        return anonymize(symbol.name(), ObjectType.SYMBOL);
    }

    @Override
    public String anonymizeColumn(String symbol)
    {
        return anonymize(symbol, ObjectType.COLUMN);
    }

    @Override
    public String anonymize(Expression expression)
    {
        return anonymizeExpressionFormatter.process(expression);
    }

    private String anonymizeSymbolReference(Reference node)
    {
        return '"' + anonymize(Symbol.from(node)) + '"';
    }

    private String anonymizeLiteral(Constant literal)
    {
        if (literal.value() == null) {
            return "null";
        }
        if (literal.type().equals(BOOLEAN)) {
            return literal.value().toString();
        }
        return anonymizeLiteral(literal.type().getDisplayName(), literal.value());
    }

    private <T> String anonymizeLiteral(String type, T value)
    {
        return "'" + type + "_" + anonymize(value, ObjectType.LITERAL) + "'";
    }

    @Override
    public String anonymize(ColumnHandle columnHandle)
    {
        return anonymize(columnHandle, ObjectType.COLUMN);
    }

    @Override
    public String anonymize(QualifiedObjectName objectName)
    {
        return anonymize(objectName.catalogName(), objectName.schemaName(), objectName.objectName());
    }

    @Override
    public String anonymize(ArgumentBinding argument)
    {
        if (argument.getConstant() != null) {
            return argument.getConstant().toString();
        }
        return anonymize(argument.getExpression());
    }

    @Override
    public String anonymize(IndexHandle indexHandle)
    {
        return formatMap(ImmutableMap.of(
                "catalog", anonymize(indexHandle.catalogHandle().getCatalogName().toString(), ObjectType.CATALOG),
                "connectorHandleType", indexHandle.connectorHandle().getClass().getSimpleName()));
    }

    @Override
    public String anonymize(TableHandle tableHandle, TableInfo tableInfo)
    {
        ImmutableMap.Builder<String, String> result = ImmutableMap.<String, String>builder()
                .put("table", anonymize(tableInfo.getTableName()));
        tableInfo.getConnectorName().ifPresent(connector -> result.put("connector", connector));
        return formatMap(result.buildOrThrow());
    }

    @Override
    public String anonymize(PartitioningHandle partitioningHandle)
    {
        ConnectorPartitioningHandle connectorHandle = partitioningHandle.getConnectorHandle();
        ImmutableMap.Builder<String, String> result = ImmutableMap.<String, String>builder()
                .put("connectorHandleType", connectorHandle.getClass().getSimpleName());
        partitioningHandle.getCatalogHandle()
                .ifPresent(catalog -> result.put("catalog", anonymize(catalog.getCatalogName().toString(), ObjectType.CATALOG)));

        if (connectorHandle instanceof SystemPartitioningHandle systemPartitioningHandle) {
            result.put("partitioning", systemPartitioningHandle.getPartitioningName())
                    .put("function", systemPartitioningHandle.getFunction().name());
        }
        return formatMap(result.buildOrThrow());
    }

    @Override
    public String anonymize(WriterTarget target)
    {
        if (target instanceof CreateTarget createTarget) {
            return anonymize(createTarget);
        }
        if (target instanceof InsertTarget insertTarget) {
            return anonymize(insertTarget);
        }
        if (target instanceof MergeTarget mergeTarget) {
            return anonymize(mergeTarget);
        }
        if (target instanceof RefreshMaterializedViewTarget refreshMaterializedViewTarget) {
            return anonymize(refreshMaterializedViewTarget);
        }
        if (target instanceof TableExecuteTarget tableExecuteTarget) {
            return anonymize(tableExecuteTarget);
        }
        throw new UnsupportedOperationException("Anonymization is not supported for WriterTarget type: " + target.getClass().getSimpleName());
    }

    @Override
    public String anonymize(WriteStatisticsTarget target)
    {
        if (target instanceof WriteStatisticsHandle writeStatisticsHandle) {
            return anonymize(
                    writeStatisticsHandle.getHandle().catalogHandle().getCatalogName().toString(),
                    ObjectType.CATALOG);
        }
        throw new UnsupportedOperationException("Anonymization is not supported for WriterTarget type: " + target.getClass().getSimpleName());
    }

    @Override
    public String anonymize(TableHandle tableHandle)
    {
        return anonymize(tableHandle.catalogHandle().getCatalogName().toString(), ObjectType.CATALOG);
    }

    @Override
    public String anonymize(TableExecuteHandle tableHandle)
    {
        return anonymize(tableHandle.catalogHandle().getCatalogName().toString(), ObjectType.CATALOG);
    }

    private String anonymize(CreateTarget target)
    {
        return anonymize(
                target.getHandle().catalogHandle().getCatalogName().toString(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(InsertTarget target)
    {
        return anonymize(
                target.getHandle().catalogHandle().getCatalogName().toString(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(MergeTarget target)
    {
        return anonymize(
                target.getHandle().catalogHandle().getCatalogName().toString(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(RefreshMaterializedViewTarget target)
    {
        return anonymize(
                target.getInsertHandle().catalogHandle().getCatalogName().toString(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(TableExecuteTarget target)
    {
        return anonymize(
                target.getExecuteHandle().catalogHandle().getCatalogName().toString(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(String catalogName, String schemaName, String objectName)
    {
        return new QualifiedObjectName(
                anonymize(catalogName, ObjectType.CATALOG),
                anonymize(schemaName, ObjectType.SCHEMA),
                anonymize(objectName, ObjectType.TABLE))
                .toString();
    }

    private <T> String anonymize(T object, ObjectType objectType)
    {
        return anonymizedMap.computeIfAbsent(objectType.name() + object, _ -> {
            Integer counter = counterMap.computeIfPresent(objectType, (k, v) -> v + 1);
            return objectType.name().toLowerCase(ENGLISH) + "_" + counter;
        });
    }

    private static String formatMap(Map<String, String> map)
    {
        return map.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue())
                .collect(joining(", ", "[", "]"));
    }
}
