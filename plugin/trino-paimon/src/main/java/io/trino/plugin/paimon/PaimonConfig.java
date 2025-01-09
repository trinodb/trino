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
package io.trino.plugin.paimon;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

public class PaimonConfig
{
    private String warehouse;
    private CatalogType catalogType;
    private boolean metadataCacheEnabled = true;

    @NotNull
    public CatalogType getCatalogType()
    {
        return catalogType;
    }

    @Config("paimon.catalog.type")
    public PaimonConfig setCatalogType(CatalogType catalogType)
    {
        this.catalogType = catalogType;
        return this;
    }

    @NotNull
    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("paimon.warehouse")
    public PaimonConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public boolean isMetadataCacheEnabled()
    {
        return metadataCacheEnabled;
    }

    @Config("paimon.metadata-cache.enabled")
    @ConfigDescription("Enables in-memory caching of metadata files on coordinator if fs.cache.enabled is not set to true")
    public PaimonConfig setMetadataCacheEnabled(boolean metadataCacheEnabled)
    {
        this.metadataCacheEnabled = metadataCacheEnabled;
        return this;
    }

    public Options toOptions()
    {
        Map<String, String> opMap = new HashMap<>();
        opMap.put("warehouse", warehouse);
        return new Options(opMap);
    }
}
