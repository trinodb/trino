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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;

import static java.util.Objects.requireNonNull;

public class DeltaLakeCacheTableId
{
    private final String schemaName;
    private final String tableName;
    private final String location;
    private final MetadataEntry metadataEntry;

    public DeltaLakeCacheTableId(
            String schemaName,
            String tableName,
            String location,
            MetadataEntry metadataEntry)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.location = requireNonNull(location, "location is null");
        this.metadataEntry = requireNonNull(metadataEntry, "metadataEntry is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public MetadataEntry getMetadataEntry()
    {
        return metadataEntry;
    }
}
