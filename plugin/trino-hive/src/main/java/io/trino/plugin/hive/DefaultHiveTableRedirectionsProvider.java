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

import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

import static io.trino.plugin.hive.HiveSessionProperties.getDeltaLakeCatalogName;
import static io.trino.plugin.hive.HiveSessionProperties.getHudiCatalogName;
import static io.trino.plugin.hive.HiveSessionProperties.getIcebergCatalogName;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;

public class DefaultHiveTableRedirectionsProvider
        implements HiveTableRedirectionsProvider
{
    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName name, Optional<Table> table)
    {
        return table.flatMap(t -> {
            Optional<String> targetCatalogName;
            if (getIcebergCatalogName(session).isPresent() && isIcebergTable(t)) {
                targetCatalogName = getIcebergCatalogName(session);
            }
            else if (getDeltaLakeCatalogName(session).isPresent() && isDeltaLakeTable(t)) {
                targetCatalogName = getDeltaLakeCatalogName(session);
            }
            else if (getHudiCatalogName(session).isPresent() && isHudiTable(t)) {
                targetCatalogName = getHudiCatalogName(session);
            }
            else {
                targetCatalogName = Optional.empty();
                System.out.println("DefaultHiveTableRedirectionsProvider:redirectTable:targetCatalogName is empty");
            }
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, t.getSchemaTableName()));
        });
    }
}
