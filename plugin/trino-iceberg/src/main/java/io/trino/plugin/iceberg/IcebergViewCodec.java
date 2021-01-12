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
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;

import java.util.Base64;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.iceberg.IcebergMetadata.STORAGE_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public class IcebergViewCodec
{
    private static final String MATERIALIZED_VIEW_PREFIX = "/* Presto Materialized View: ";
    private static final String MATERIALIZED_VIEW_SUFFIX = " */";
    private static final String PRESTO_VIEW_FLAG = "presto_view";
    private static final JsonCodec<ConnectorMaterializedViewDefinition> MATERIALIZED_VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorMaterializedViewDefinition.class);

    public String encodeMaterializedView(ConnectorMaterializedViewDefinition definition)
    {
        byte[] bytes = MATERIALIZED_VIEW_CODEC.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return MATERIALIZED_VIEW_PREFIX + data + MATERIALIZED_VIEW_SUFFIX;
    }

    public ConnectorMaterializedViewDefinition decodeMaterializedView(String data)
    {
        checkCondition(data.startsWith(MATERIALIZED_VIEW_PREFIX), HIVE_INVALID_VIEW_DATA, "Materialized View data missing prefix: %s", data);
        checkCondition(data.endsWith(MATERIALIZED_VIEW_SUFFIX), HIVE_INVALID_VIEW_DATA, "Materialized View data missing suffix: %s", data);
        data = data.substring(MATERIALIZED_VIEW_PREFIX.length());
        data = data.substring(0, data.length() - MATERIALIZED_VIEW_SUFFIX.length());
        byte[] bytes = Base64.getDecoder().decode(data);
        return MATERIALIZED_VIEW_CODEC.fromJson(bytes);
    }

    public String getPrestoViewFlag()
    {
        return PRESTO_VIEW_FLAG;
    }

    public boolean isMaterializedView(Table table)
    {
        if (table.getTableType().equals(VIRTUAL_VIEW.name()) &&
                "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG)) &&
                table.getParameters().containsKey(STORAGE_TABLE)) {
            return true;
        }
        return false;
    }
}
