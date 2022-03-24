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

import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.trino.rel2trino.RelToTrinoConverter;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.CoralSemiTransactionalHiveMSCAdapter;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
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
import org.apache.hadoop.hive.metastore.TableType;

import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_VIEW_TRANSLATION_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveSessionProperties.isLegacyHiveViewTranslation;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public final class ViewReaderUtil
{
    private ViewReaderUtil()
    {}

    public interface ViewReader
    {
        ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName);
    }

    public static ViewReader createViewReader(
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            Table table,
            TypeManager typeManager,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> tableRedirectionResolver,
            MetadataProvider metadataProvider)
    {
        if (isPrestoView(table)) {
            return new PrestoViewReader();
        }
        if (isLegacyHiveViewTranslation(session)) {
            return new LegacyHiveViewReader();
        }

        return new HiveViewReader(
                new CoralSemiTransactionalHiveMSCAdapter(metastore, coralTableRedirectionResolver(session, tableRedirectionResolver, metadataProvider)),
                typeManager);
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
                    .map(columnSchema -> new Column(columnSchema.getName(), toHiveType(columnSchema.getType()), Optional.empty() /* comment */))
                    .collect(toImmutableList());
            Table table = Table.builder()
                    .setDatabaseName(schemaTableName.getSchemaName())
                    .setTableName(schemaTableName.getTableName())
                    .setTableType(EXTERNAL_TABLE.name())
                    .setDataColumns(columns)
                    // Required by 'Table', but not used by view translation.
                    .withStorage(storage -> storage.setStorageFormat(fromHiveStorageFormat(TEXTFILE)))
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

    public static boolean isPrestoView(Table table)
    {
        return isPrestoView(table.getParameters());
    }

    public static boolean isPrestoView(Map<String, String> tableParameters)
    {
        return "true".equals(tableParameters.get(PRESTO_VIEW_FLAG));
    }

    public static boolean isHiveOrPrestoView(Table table)
    {
        return isHiveOrPrestoView(table.getTableType());
    }

    public static boolean isHiveOrPrestoView(String tableType)
    {
        return tableType.equals(TableType.VIRTUAL_VIEW.name());
    }

    public static boolean canDecodeView(Table table)
    {
        // we can decode Hive or Presto view
        return table.getTableType().equals(VIRTUAL_VIEW.name());
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
        public ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName)
        {
            return decodeViewData(viewData);
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

        public HiveViewReader(HiveMetastoreClient hiveMetastoreClient, TypeManager typeManager)
        {
            this.metastoreClient = requireNonNull(hiveMetastoreClient, "hiveMetastoreClient is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public ConnectorViewDefinition decodeViewData(String viewSql, Table table, CatalogName catalogName)
        {
            try {
                HiveToRelConverter hiveToRelConverter = new HiveToRelConverter(metastoreClient);
                RelNode rel = hiveToRelConverter.convertView(table.getDatabaseName(), table.getTableName());
                RelToTrinoConverter relToTrino = new RelToTrinoConverter();
                String trinoSql = relToTrino.convert(rel);
                RelDataType rowType = rel.getRowType();
                List<ViewColumn> columns = rowType.getFieldList().stream()
                        .map(field -> new ViewColumn(
                                field.getName(),
                                typeManager.fromSqlType(getTypeString(field.getType())).getTypeId()))
                        .collect(toImmutableList());
                return new ConnectorViewDefinition(
                        trinoSql,
                        Optional.of(catalogName.toString()),
                        Optional.of(table.getDatabaseName()),
                        columns,
                        Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)),
                        Optional.empty(),
                        false);
            }
            catch (RuntimeException e) {
                throw new TrinoException(HIVE_VIEW_TRANSLATION_ERROR,
                        format("Failed to translate Hive view '%s': %s",
                                table.getSchemaTableName(),
                                e.getMessage()),
                        e);
            }
        }

        // Calcite does not provide correct type strings for non-primitive types.
        // We add custom code here to make it work. Goal is for calcite/coral to handle this
        private String getTypeString(RelDataType type)
        {
            switch (type.getSqlTypeName()) {
                case ROW: {
                    verify(type.isStruct(), "expected ROW type to be a struct: %s", type);
                    return type.getFieldList().stream()
                            .map(field -> field.getName().toLowerCase(Locale.ENGLISH) + " " + getTypeString(field.getType()))
                            .collect(joining(",", "row(", ")"));
                }
                case CHAR:
                    return "varchar";
                case FLOAT:
                    return "real";
                case BINARY:
                case VARBINARY:
                    return "varbinary";
                case MAP: {
                    RelDataType keyType = type.getKeyType();
                    RelDataType valueType = type.getValueType();
                    return format("map(%s,%s)", getTypeString(keyType), getTypeString(valueType));
                }
                case ARRAY: {
                    return format("array(%s)", getTypeString(type.getComponentType()));
                }
                case DECIMAL: {
                    return format("decimal(%s,%s)", type.getPrecision(), type.getScale());
                }
                default:
                    return type.getSqlTypeName().toString();
            }
        }
    }
}
