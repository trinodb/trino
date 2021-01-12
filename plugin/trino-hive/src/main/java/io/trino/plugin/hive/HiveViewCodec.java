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

import com.linkedin.coral.hive.hive2rel.HiveMetastoreClient;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.presto.rel2presto.RelToPrestoConverter;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.CoralSemiTransactionalHiveMSCAdapter;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import javax.inject.Inject;

import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_VIEW_TRANSLATION_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveSessionProperties.isLegacyHiveViewTranslation;
import static io.trino.plugin.hive.HiveToTrinoTranslator.translateHiveViewToTrino;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public final class HiveViewCodec
{
    private static final String VIEW_FLAG_PATTERN = "%s_view";
    private static final String VIEW_PREFIX_PATTERN = "/* %s View: ";
    private static final String VIEW_SUFFIX_PATTERN = " */";
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorViewDefinition.class);

    private final boolean translateHiveViews;

    private static final TrinoViewConstants DEFAULT_VIEW_CONSTANTS = TrinoViewConstants.create("Trino");
    private final Optional<TrinoViewConstants> alternateViewConstants;

    @Inject
    public HiveViewCodec(HiveConfig hiveConfig)
    {
        requireNonNull(hiveConfig, "hiveConfig is null");
        this.translateHiveViews = hiveConfig.isTranslateHiveViews();
        this.alternateViewConstants = hiveConfig.getAlternateViewCodecTag().map(TrinoViewConstants::create);
    }

    public String encodeView(ConnectorViewDefinition definition)
    {
        byte[] bytes = VIEW_CODEC.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return DEFAULT_VIEW_CONSTANTS.prefix() + data + DEFAULT_VIEW_CONSTANTS.suffix();
    }

    public ConnectorViewDefinition decodeView(
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            Table view,
            TypeManager typeManager,
            CatalogName catalogName)
    {
        if (!translateHiveViews && !isTrinoView(view)) {
            throw new HiveViewNotSupportedException(new SchemaTableName(view.getDatabaseName(), view.getTableName()));
        }

        ConnectorViewDefinition definition = createViewReader(metastore, session, view, typeManager)
                .decodeViewData(view.getViewOriginalText().get(), view, catalogName);

        // use owner from table metadata if it exists
        if (view.getOwner() != null && !definition.isRunAsInvoker()) {
            definition = new ConnectorViewDefinition(
                    definition.getOriginalSql(),
                    definition.getCatalog(),
                    definition.getSchema(),
                    definition.getColumns(),
                    definition.getComment(),
                    Optional.of(view.getOwner()),
                    false);
        }

        return definition;
    }

    public boolean canDecodeView(Table table)
    {
        // we can decode Hive or Trino view
        return table.getTableType().equals(VIRTUAL_VIEW.name());
    }

    public boolean isTranslateHiveView()
    {
        return translateHiveViews;
    }

    public String getTrinoViewFlag()
    {
        return DEFAULT_VIEW_CONSTANTS.flag();
    }

    public Optional<String> getAlternateTrinoViewFlag()
    {
        return alternateViewConstants.map(TrinoViewConstants::flag);
    }

    public boolean isTrinoView(Table table)
    {
        return getTrinoViewConstantsFor(table).isPresent();
    }

    private Optional<TrinoViewConstants> getTrinoViewConstantsFor(Table table)
    {
        if ("true".equals(table.getParameters().get(DEFAULT_VIEW_CONSTANTS.flag()))) {
            return Optional.of(DEFAULT_VIEW_CONSTANTS);
        }

        if ("true".equals(
                alternateViewConstants
                        .map(TrinoViewConstants::flag)
                        .map(flag -> table.getParameters().get(flag))
                        .orElse(null))) {
            return alternateViewConstants;
        }

        return Optional.empty();
    }

    private interface ViewReader
    {
        ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName);
    }

    private ViewReader createViewReader(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table, TypeManager typemanager)
    {
        if (isTrinoView(table)) {
            return new TrinoViewReader();
        }
        if (isLegacyHiveViewTranslation(session)) {
            return new LegacyHiveViewReader();
        }

        return new HiveViewReader(new CoralSemiTransactionalHiveMSCAdapter(metastore, new HiveIdentity(session)), typemanager);
    }

    /**
     * Supports decoding of Trino views
     */
    private class TrinoViewReader
            implements ViewReader
    {
        @Override
        public ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName)
        {
            Optional<TrinoViewConstants> optionalTrinoViewConstants = getTrinoViewConstantsFor(table);
            checkArgument(optionalTrinoViewConstants.isPresent(), "Not a Trino view");
            TrinoViewConstants viewConstants = optionalTrinoViewConstants.get();

            checkCondition(viewData.startsWith(viewConstants.prefix()), HIVE_INVALID_VIEW_DATA, "View data missing prefix: %s", viewData);
            checkCondition(viewData.endsWith(viewConstants.suffix()), HIVE_INVALID_VIEW_DATA, "View data missing suffix: %s", viewData);
            viewData = viewData.substring(viewConstants.prefix().length());
            viewData = viewData.substring(0, viewData.length() - viewConstants.suffix().length());
            byte[] bytes = Base64.getDecoder().decode(viewData);
            return VIEW_CODEC.fromJson(bytes);
        }
    }

    /**
     * Class to decode Hive view definitions
     */
    private static class HiveViewReader
            implements ViewReader
    {
        private final HiveMetastoreClient metastoreClient;
        private final TypeManager typeManager;

        public HiveViewReader(HiveMetastoreClient hiveMetastoreClient, TypeManager typemanager)
        {
            this.metastoreClient = requireNonNull(hiveMetastoreClient, "metastoreClient is null");
            this.typeManager = requireNonNull(typemanager, "typeManager is null");
        }

        @Override
        public ConnectorViewDefinition decodeViewData(String viewSql, Table table, CatalogName catalogName)
        {
            try {
                HiveToRelConverter hiveToRelConverter = HiveToRelConverter.create(metastoreClient);
                RelNode rel = hiveToRelConverter.convertView(table.getDatabaseName(), table.getTableName());
                RelToPrestoConverter rel2Presto = new RelToPrestoConverter();
                String trinoSql = rel2Presto.convert(rel);
                RelDataType rowType = rel.getRowType();
                List<ViewColumn> columns = rowType.getFieldList().stream()
                        .map(field -> new ViewColumn(
                                field.getName(),
                                typeManager.fromSqlType(getTypeString(field.getType())).getTypeId()))
                        .collect(toImmutableList());
                return new ConnectorViewDefinition(trinoSql,
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

    /**
     * Class to decode hive view definitions using a legacy approach
     */
    private static class LegacyHiveViewReader
            implements HiveViewCodec.ViewReader
    {
        @Override
        public ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName)
        {
            String viewText = table.getViewExpandedText()
                    .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view expanded text: " + table.getSchemaTableName()));
            return new ConnectorViewDefinition(
                    translateHiveViewToTrino(viewText),
                    Optional.of(catalogName.toString()),
                    Optional.ofNullable(table.getDatabaseName()),
                    table.getDataColumns().stream()
                            .map(column -> new ConnectorViewDefinition.ViewColumn(column.getName(), TypeId.of(column.getType().getTypeSignature().toString())))
                            .collect(toImmutableList()),
                    Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)),
                    Optional.of(table.getOwner()),
                    false); // don't run as invoker
        }
    }

    private static class TrinoViewConstants
    {
        private final String flag;
        private final String prefix;
        private final String suffix;

        private TrinoViewConstants(String flag, String prefix, String suffix)
        {
            this.flag = requireNonNull(flag, "flag is null");
            this.prefix = requireNonNull(prefix, "prefix is null");
            this.suffix = requireNonNull(suffix, "suffix is null");
        }

        public static TrinoViewConstants create(String tag)
        {
            return new TrinoViewConstants(
                    format(VIEW_FLAG_PATTERN, tag.toLowerCase(Locale.ENGLISH)),
                    format(VIEW_PREFIX_PATTERN, tag),
                    VIEW_SUFFIX_PATTERN);
        }

        public String flag()
        {
            return flag;
        }

        public String prefix()
        {
            return prefix;
        }

        public String suffix()
        {
            return suffix;
        }
    }
}
