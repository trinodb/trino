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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.trino.rel2trino.RelToTrinoConverter;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.metastore.CoralSemiTransactionalHiveMSCAdapter;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.linkedin.coral.trino.rel2trino.functions.TrinoKeywordsConverter.quoteWordIfNotQuoted;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.TableInfo.PRESTO_VIEW_COMMENT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_VIEW_TRANSLATION_ERROR;
import static io.trino.plugin.hive.HiveSessionProperties.isHiveViewsLegacyTranslation;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ViewReaderUtil
{
    private ViewReaderUtil()
    {}

    public interface ViewReader
    {
        ConnectorViewDefinition decodeViewData(Optional<String> viewData, Table table, CatalogName catalogName);
    }

    public static ViewReader createViewReader(
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            Table table,
            TypeManager typeManager,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> tableRedirectionResolver,
            MetadataProvider metadataProvider,
            boolean runHiveViewRunAsInvoker,
            HiveTimestampPrecision hiveViewsTimestampPrecision)
    {
        if (isTrinoView(table)) {
            return new PrestoViewReader();
        }
        if (isHiveViewsLegacyTranslation(session)) {
            return new LegacyHiveViewReader(runHiveViewRunAsInvoker);
        }

        return new HiveViewReader(
                new CoralSemiTransactionalHiveMSCAdapter(metastore, coralTableRedirectionResolver(session, tableRedirectionResolver, metadataProvider)),
                typeManager,
                runHiveViewRunAsInvoker,
                hiveViewsTimestampPrecision);
    }

    private static CoralTableRedirectionResolver coralTableRedirectionResolver(
            ConnectorSession session,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> tableRedirectionResolver,
            MetadataProvider metadataProvider)
    {
        return schemaTableName -> tableRedirectionResolver.apply(session, schemaTableName).map(target -> {
            ConnectorTableSchema tableSchema = metadataProvider.getRelationMetadata(session, target)
                    .orElseThrow(() -> new TableNotFoundException(
                            target.getSchemaTableName(),
                            format("%s is redirected to %s, but that relation cannot be found", schemaTableName, target)));
            List<Column> columns = tableSchema.getColumns().stream()
                    .filter(columnSchema -> !columnSchema.isHidden())
                    .map(columnSchema -> new Column(columnSchema.getName(), toHiveType(columnSchema.getType()), Optional.empty() /* comment */, Map.of()))
                    .collect(toImmutableList());
            Table table = Table.builder()
                    .setDatabaseName(schemaTableName.getSchemaName())
                    .setTableName(schemaTableName.getTableName())
                    .setTableType(EXTERNAL_TABLE.name())
                    .setDataColumns(columns)
                    // Required by 'Table', but not used by view translation.
                    .withStorage(storage -> storage.setStorageFormat(TEXTFILE.toStorageFormat()))
                    .setOwner(Optional.empty())
                    .build();
            return toMetastoreApiTable(table);
        });
    }

    public static final String PRESTO_VIEW_FLAG = "presto_view";
    static final String VIEW_PREFIX = "/* Presto View: ";
    static final String VIEW_SUFFIX = " */";
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorViewDefinition.class);

    /**
     * Returns true if table represents a Hive view, Trino/Presto view, materialized view or anything
     * else that gets registered using table type "VIRTUAL_VIEW".
     * Note: this method returns false for a table that represents Hive's own materialized view
     * ("MATERIALIZED_VIEW" table type). Hive own's materialized views are currently treated as ordinary
     * tables by Trino.
     */
    public static boolean isSomeKindOfAView(Table table)
    {
        return table.getTableType().equals(VIRTUAL_VIEW.name());
    }

    public static boolean isHiveView(Table table)
    {
        return table.getTableType().equals(VIRTUAL_VIEW.name()) &&
                !table.getParameters().containsKey(PRESTO_VIEW_FLAG);
    }

    /**
     * Returns true when the table represents a "Trino view" (AKA "presto view").
     * Returns false for Hive views or Trino materialized views.
     */
    public static boolean isTrinoView(Table table)
    {
        return isTrinoView(table.getTableType(), table.getParameters());
    }

    /**
     * Returns true when the table represents a "Trino view" (AKA "presto view").
     * Returns false for Hive views or Trino materialized views.
     */
    public static boolean isTrinoView(String tableType, Map<String, String> tableParameters)
    {
        // A Trino view can be recognized by table type "VIRTUAL_VIEW" and table parameters presto_view="true" and comment="Presto View" since their first implementation see
        // https://github.com/trinodb/trino/blame/38bd0dff736024f3ae01dbbe7d1db5bd1d50c43e/presto-hive/src/main/java/com/facebook/presto/hive/HiveMetadata.java#L902.
        return tableType.equals(VIRTUAL_VIEW.name()) &&
                "true".equals(tableParameters.get(PRESTO_VIEW_FLAG)) &&
                PRESTO_VIEW_COMMENT.equalsIgnoreCase(tableParameters.get(TABLE_COMMENT));
    }

    public static boolean isTrinoMaterializedView(Table table)
    {
        return isTrinoMaterializedView(table.getTableType(), table.getParameters());
    }

    public static boolean isTrinoMaterializedView(String tableType, Map<String, String> tableParameters)
    {
        // A Trino materialized view can be recognized by table type "VIRTUAL_VIEW" and table parameters presto_view="true" and comment="Presto Materialized View"
        // since their first implementation see
        // https://github.com/trinodb/trino/blame/ff4a1e31fb9cb49f1b960abfc16ad469e7126a64/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java#L898
        return tableType.equals(VIRTUAL_VIEW.name()) &&
                "true".equals(tableParameters.get(PRESTO_VIEW_FLAG)) &&
                TableInfo.ICEBERG_MATERIALIZED_VIEW_COMMENT.equalsIgnoreCase(tableParameters.get(TABLE_COMMENT));
    }

    public static String encodeViewData(ConnectorViewDefinition definition)
    {
        byte[] bytes = VIEW_CODEC.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return VIEW_PREFIX + data + VIEW_SUFFIX;
    }

    /**
     * Supports decoding of Presto views
     */
    public static class PrestoViewReader
            implements ViewReader
    {
        @Override
        public ConnectorViewDefinition decodeViewData(Optional<String> viewData, Table table, CatalogName catalogName)
        {
            return decodeViewData(viewData.orElseThrow(() -> new TrinoException(HIVE_VIEW_TRANSLATION_ERROR, "Cannot decode view data, view original sql is empty")));
        }

        public static ConnectorViewDefinition decodeViewData(String viewData)
        {
            checkCondition(viewData.startsWith(VIEW_PREFIX), HIVE_INVALID_VIEW_DATA, "View data missing prefix: %s", viewData);
            checkCondition(viewData.endsWith(VIEW_SUFFIX), HIVE_INVALID_VIEW_DATA, "View data missing suffix: %s", viewData);
            viewData = viewData.substring(VIEW_PREFIX.length());
            viewData = viewData.substring(0, viewData.length() - VIEW_SUFFIX.length());
            byte[] bytes = Base64.getDecoder().decode(viewData);
            return VIEW_CODEC.fromJson(bytes);
        }
    }

    /**
     * Class to decode Hive view definitions
     */
    public static class HiveViewReader
            implements ViewReader
    {
        private final HiveMetastoreClient metastoreClient;
        private final TypeManager typeManager;
        private final boolean hiveViewsRunAsInvoker;
        private final HiveTimestampPrecision hiveViewsTimestampPrecision;

        public HiveViewReader(HiveMetastoreClient hiveMetastoreClient, TypeManager typeManager, boolean hiveViewsRunAsInvoker, HiveTimestampPrecision hiveViewsTimestampPrecision)
        {
            this.metastoreClient = requireNonNull(hiveMetastoreClient, "hiveMetastoreClient is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.hiveViewsRunAsInvoker = hiveViewsRunAsInvoker;
            this.hiveViewsTimestampPrecision = requireNonNull(hiveViewsTimestampPrecision, "hiveViewsTimestampPrecision is null");
        }

        @Override
        public ConnectorViewDefinition decodeViewData(Optional<String> viewSql, Table table, CatalogName catalogName)
        {
            try {
                HiveToRelConverter hiveToRelConverter = new HiveToRelConverter(metastoreClient);
                RelNode rel = hiveToRelConverter.convertView(table.getDatabaseName(), table.getTableName());
                RelToTrinoConverter relToTrino = new RelToTrinoConverter(metastoreClient);
                String trinoSql = relToTrino.convert(rel);
                RelDataType rowType = rel.getRowType();

                Map<String, String> columnComments = Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream())
                        .filter(column -> column.getComment().isPresent())
                        .collect(toImmutableMap(Column::getName, column -> column.getComment().get()));

                List<ViewColumn> columns = rowType.getFieldList().stream()
                        .map(field -> new ViewColumn(
                                field.getName(),
                                typeManager.fromSqlType(getTypeString(field.getType(), hiveViewsTimestampPrecision)).getTypeId(),
                                Optional.ofNullable(columnComments.get(field.getName()))))
                        .collect(toImmutableList());
                return new ConnectorViewDefinition(
                        trinoSql,
                        Optional.of(catalogName.toString()),
                        Optional.of(table.getDatabaseName()),
                        columns,
                        Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)),
                        Optional.empty(), // will be filled in later by HiveMetadata
                        hiveViewsRunAsInvoker,
                        ImmutableList.of());
            }
            catch (Throwable e) {
                throw new TrinoException(HIVE_VIEW_TRANSLATION_ERROR,
                        format("Failed to translate Hive view '%s': %s",
                                table.getSchemaTableName(),
                                e.getMessage()),
                        e);
            }
        }

        // Calcite does not provide correct type strings for non-primitive types.
        // We add custom code here to make it work. Goal is for calcite/coral to handle this
        private static String getTypeString(RelDataType type, HiveTimestampPrecision timestampPrecision)
        {
            switch (type.getSqlTypeName()) {
                case ROW: {
                    verify(type.isStruct(), "expected ROW type to be a struct: %s", type);
                    // There is no API in RelDataType for Coral to add quotes for rowType.
                    // We add the Coral function here to parse data types successfully.
                    // Goal is to use data type mapping instead of translating to strings
                    return type.getFieldList().stream()
                            .map(field -> quoteWordIfNotQuoted(field.getName().toLowerCase(Locale.ENGLISH)) + " " + getTypeString(field.getType(), timestampPrecision))
                            .collect(joining(",", "row(", ")"));
                }
                case CHAR:
                    return "char(" + type.getPrecision() + ")";
                case FLOAT:
                    return "real";
                case BINARY:
                case VARBINARY:
                    return "varbinary";
                case MAP: {
                    RelDataType keyType = type.getKeyType();
                    RelDataType valueType = type.getValueType();
                    return "map(" + getTypeString(keyType, timestampPrecision) + "," + getTypeString(valueType, timestampPrecision) + ")";
                }
                case ARRAY: {
                    return "array(" + getTypeString(type.getComponentType(), timestampPrecision) + ")";
                }
                case DECIMAL: {
                    return "decimal(" + type.getPrecision() + "," + type.getScale() + ")";
                }
                case TIMESTAMP:
                    return "timestamp(" + timestampPrecision.getPrecision() + ")";
                default:
                    return type.getSqlTypeName().toString();
            }
        }
    }
}
