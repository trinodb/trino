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
package io.trino.plugin.iceberg;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;

import javax.inject.Inject;

import java.util.Base64;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.iceberg.IcebergMetadata.STORAGE_TABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public class IcebergViewCodec
{
    private static final String MATERIALIZED_VIEW_PREFIX_PATTERN = "/* %s Materialized View: ";
    private static final String MATERIALIZED_VIEW_SUFFIX_PATTERN = " */";
    private static final String VIEW_FLAG_PATTERN = "%s_view";
    private static final JsonCodec<ConnectorMaterializedViewDefinition> MATERIALIZED_VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorMaterializedViewDefinition.class);

    private static final MaterializedViewConstants DEFAULT_MATERIALIZED_VIEW_CONSTANTS = MaterializedViewConstants.create("Trino");
    private final Optional<MaterializedViewConstants> alternateViewConstants;

    @Inject
    public IcebergViewCodec(IcebergConfig config)
    {
        requireNonNull(config, "config is null");
        alternateViewConstants = config.getAlternateViewCodecTag().map(MaterializedViewConstants::create);
    }

    public String encodeMaterializedView(ConnectorMaterializedViewDefinition definition)
    {
        byte[] bytes = MATERIALIZED_VIEW_CODEC.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return DEFAULT_MATERIALIZED_VIEW_CONSTANTS.prefix() + data + DEFAULT_MATERIALIZED_VIEW_CONSTANTS.suffix();
    }

    public ConnectorMaterializedViewDefinition decodeMaterializedView(Table table)
    {
        Optional<MaterializedViewConstants> optionalConstants = getMaterializedViewConstantsFor(table);
        checkCondition(optionalConstants.isPresent(), HIVE_INVALID_VIEW_DATA, "not a valid materialized view");
        MaterializedViewConstants constants = optionalConstants.get();

        String viewText = table.getViewExpandedText()
                .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + table.getSchemaTableName()));

        checkCondition(viewText.startsWith(constants.prefix()), HIVE_INVALID_VIEW_DATA, "Materialized View data missing prefix: %s", viewText);
        checkCondition(viewText.endsWith(constants.suffix()), HIVE_INVALID_VIEW_DATA, "Materialized View data missing suffix: %s", viewText);
        viewText = viewText.substring(constants.prefix().length());
        viewText = viewText.substring(0, viewText.length() - constants.suffix().length());
        byte[] bytes = Base64.getDecoder().decode(viewText);
        return MATERIALIZED_VIEW_CODEC.fromJson(bytes);
    }

    private Optional<MaterializedViewConstants> getMaterializedViewConstantsFor(Table table)
    {
        if ("true".equals(table.getParameters().get(DEFAULT_MATERIALIZED_VIEW_CONSTANTS.flag()))) {
            return Optional.of(DEFAULT_MATERIALIZED_VIEW_CONSTANTS);
        }

        if ("true".equals(alternateViewConstants
                .map(MaterializedViewConstants::flag)
                .map(flag -> table.getParameters().get(flag))
                .orElse(null))) {
            return alternateViewConstants;
        }

        return Optional.empty();
    }

    public String getTrinoViewFlag()
    {
        return DEFAULT_MATERIALIZED_VIEW_CONSTANTS.flag();
    }

    public boolean isMaterializedView(Table table)
    {
        if (table.getTableType().equals(VIRTUAL_VIEW.name()) &&
                ("true".equals(table.getParameters().get(DEFAULT_MATERIALIZED_VIEW_CONSTANTS.flag)) ||
                        "true".equals(alternateViewConstants
                                .map(MaterializedViewConstants::flag)
                                .map(flag -> table.getParameters().get(flag))
                                .orElse(null))) &&
                table.getParameters().containsKey(STORAGE_TABLE)) {
            return true;
        }
        return false;
    }

    private static class MaterializedViewConstants
    {
        private final String flag;
        private final String prefix;
        private final String suffix;

        private MaterializedViewConstants(String flag, String prefix, String suffix)
        {
            this.flag = requireNonNull(flag, "flag is null");
            this.prefix = requireNonNull(prefix, "prefix is null");
            this.suffix = requireNonNull(suffix, "suffix is null");
        }

        public static MaterializedViewConstants create(String tag)
        {
            return new MaterializedViewConstants(
                    format(VIEW_FLAG_PATTERN, tag.toLowerCase(Locale.ENGLISH)),
                    format(MATERIALIZED_VIEW_PREFIX_PATTERN, tag),
                    MATERIALIZED_VIEW_SUFFIX_PATTERN);
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
