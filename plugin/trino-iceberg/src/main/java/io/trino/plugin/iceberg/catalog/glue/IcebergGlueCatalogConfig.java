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
package io.trino.plugin.iceberg.catalog.glue;

import io.airlift.configuration.Config;

public class IcebergGlueCatalogConfig
{
    private boolean cacheTableMetadata = true;

    public boolean isCacheTableMetadata()
    {
        return cacheTableMetadata;
    }

    @Config("iceberg.glue.cache-table-metadata")
    public IcebergGlueCatalogConfig setCacheTableMetadata(boolean cacheTableMetadata)
    {
        this.cacheTableMetadata = cacheTableMetadata;
        return this;
    }
}
