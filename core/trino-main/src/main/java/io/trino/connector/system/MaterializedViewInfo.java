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
package io.trino.connector.system;

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;

public class MaterializedViewInfo
{
    private final QualifiedObjectName materializedView;
    private final ConnectorMaterializedViewDefinition definition;
    private final MaterializedViewFreshness freshness;

    public MaterializedViewInfo(
            QualifiedObjectName materializedView,
            ConnectorMaterializedViewDefinition definition,
            MaterializedViewFreshness freshness)
    {
        this.materializedView = materializedView;
        this.definition = definition;
        this.freshness = freshness;
    }

    public boolean isFresh()
    {
        return freshness.isMaterializedViewFresh();
    }

    public String getName()
    {
        return materializedView.getObjectName();
    }

    public String getCatalogName()
    {
        return materializedView.getCatalogName();
    }

    public String getSchemaName()
    {
        return materializedView.getSchemaName();
    }

    public String getStorageCatalog()
    {
        return definition.getStorageTable()
                .map(CatalogSchemaTableName::getCatalogName)
                .orElse("");
    }

    public String getStorageSchema()
    {
        return definition.getStorageTable()
                .map(storageTable -> storageTable.getSchemaTableName().getSchemaName())
                .orElse("");
    }

    public String getStorageTable()
    {
        return definition.getStorageTable()
                .map(storageTable -> storageTable.getSchemaTableName().getTableName())
                .orElse("");
    }

    public String getOwner()
    {
        return definition.getOwner();
    }

    public String getComment()
    {
        return definition.getComment().orElse("");
    }

    public String getOriginalSql()
    {
        return definition.getOriginalSql();
    }
}
