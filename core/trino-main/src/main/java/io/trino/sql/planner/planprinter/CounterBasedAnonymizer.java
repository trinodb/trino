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
import io.trino.sql.ExpressionFormatter;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.Partitioning.ArgumentBinding;
import static io.trino.sql.planner.plan.StatisticsWriterNode.WriteStatisticsHandle;
import static io.trino.sql.planner.plan.StatisticsWriterNode.WriteStatisticsTarget;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import static io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewTarget;
import static io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static java.nio.charset.StandardCharsets.UTF_8;
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
        return anonymize(symbol.getName(), ObjectType.SYMBOL);
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

    private String anonymizeSymbolReference(SymbolReference node)
    {
        return '"' + anonymize(Symbol.from(node)) + '"';
    }

    private String anonymizeLiteral(Literal node)
    {
        if (node instanceof StringLiteral literal) {
            return anonymizeLiteral("string", literal.getValue());
        }
        if (node instanceof GenericLiteral literal) {
            return anonymizeLiteral(literal.getType(), literal.getValue());
        }
        if (node instanceof CharLiteral literal) {
            return anonymizeLiteral("char", literal.getValue());
        }
        if (node instanceof BinaryLiteral literal) {
            return anonymizeLiteral("binary", new String(literal.getValue(), UTF_8));
        }
        if (node instanceof DecimalLiteral literal) {
            return anonymizeLiteral("decimal", literal.getValue());
        }
        if (node instanceof DoubleLiteral literal) {
            return anonymizeLiteral("double", literal.getValue());
        }
        if (node instanceof LongLiteral literal) {
            return anonymizeLiteral("long", literal.getParsedValue());
        }
        if (node instanceof TimestampLiteral literal) {
            return anonymizeLiteral("timestamp", literal.getValue());
        }
        if (node instanceof TimeLiteral literal) {
            return anonymizeLiteral("time", literal.getValue());
        }
        if (node instanceof IntervalLiteral literal) {
            return anonymizeLiteral("interval", literal.getValue());
        }
        if (node instanceof BooleanLiteral literal) {
            return String.valueOf(literal.getValue());
        }
        if (node instanceof NullLiteral) {
            return "null";
        }
        throw new UnsupportedOperationException("Anonymization is not supported for literal " + node);
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
        return anonymize(objectName.getCatalogName(), objectName.getSchemaName(), objectName.getObjectName());
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
                "catalog", anonymize(indexHandle.getCatalogHandle().getCatalogName(), ObjectType.CATALOG),
                "connectorHandleType", indexHandle.getConnectorHandle().getClass().getSimpleName()));
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
                .ifPresent(catalog -> result.put("catalog", anonymize(catalog.getCatalogName(), ObjectType.CATALOG)));

        if (connectorHandle instanceof SystemPartitioningHandle) {
            result.put("partitioning", ((SystemPartitioningHandle) connectorHandle).getPartitioningName())
                    .put("function", ((SystemPartitioningHandle) connectorHandle).getFunction().name());
        }
        return formatMap(result.buildOrThrow());
    }

    @Override
    public String anonymize(WriterTarget target)
    {
        if (target instanceof CreateTarget) {
            return anonymize((CreateTarget) target);
        }
        if (target instanceof InsertTarget) {
            return anonymize((InsertTarget) target);
        }
        if (target instanceof MergeTarget) {
            return anonymize((MergeTarget) target);
        }
        if (target instanceof RefreshMaterializedViewTarget) {
            return anonymize((RefreshMaterializedViewTarget) target);
        }
        if (target instanceof TableExecuteTarget) {
            return anonymize((TableExecuteTarget) target);
        }
        throw new UnsupportedOperationException("Anonymization is not supported for WriterTarget type: " + target.getClass().getSimpleName());
    }

    @Override
    public String anonymize(WriteStatisticsTarget target)
    {
        if (target instanceof WriteStatisticsHandle) {
            return anonymize(
                    ((WriteStatisticsHandle) target).getHandle().getCatalogHandle().getCatalogName(),
                    ObjectType.CATALOG);
        }
        throw new UnsupportedOperationException("Anonymization is not supported for WriterTarget type: " + target.getClass().getSimpleName());
    }

    @Override
    public String anonymize(TableHandle tableHandle)
    {
        return anonymize(tableHandle.getCatalogHandle().getCatalogName(), ObjectType.CATALOG);
    }

    @Override
    public String anonymize(TableExecuteHandle tableHandle)
    {
        return anonymize(tableHandle.getCatalogHandle().getCatalogName(), ObjectType.CATALOG);
    }

    private String anonymize(CreateTarget target)
    {
        return anonymize(
                target.getHandle().getCatalogHandle().getCatalogName(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(InsertTarget target)
    {
        return anonymize(
                target.getHandle().getCatalogHandle().getCatalogName(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(MergeTarget target)
    {
        return anonymize(
                target.getHandle().getCatalogHandle().getCatalogName(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(RefreshMaterializedViewTarget target)
    {
        return anonymize(
                target.getInsertHandle().getCatalogHandle().getCatalogName(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName());
    }

    private String anonymize(TableExecuteTarget target)
    {
        return anonymize(
                target.getExecuteHandle().getCatalogHandle().getCatalogName(),
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
        return anonymizedMap.computeIfAbsent(objectType.name() + object, ignored -> {
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
