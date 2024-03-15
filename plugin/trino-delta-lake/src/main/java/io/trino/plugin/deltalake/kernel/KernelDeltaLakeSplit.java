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
package io.trino.plugin.deltalake.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.trino.spi.connector.ConnectorSplit;

import static io.trino.plugin.deltalake.kernel.KernelRowSerDeUtils.deserializeRowFromJson;

public class KernelDeltaLakeSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final String location;
    private final String serializedScanState;
    private final String serializedScanFile;

    @JsonCreator
    public KernelDeltaLakeSplit(
            String schemaName,
            String tableName,
            String location,
            String serializedScanState,
            String serializedScanFile)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.location = location;
        this.serializedScanState = serializedScanState;
        this.serializedScanFile = serializedScanFile;
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
    public String getSerializedScanState()
    {
        return serializedScanState;
    }

    @JsonProperty
    public String getSerializedScanFile()
    {
        return serializedScanFile;
    }

    @JsonIgnore
    public Row getScanState(Engine engine)
    {
        return deserializeRowFromJson(engine, serializedScanState);
    }

    @JsonIgnore
    public Row getScanFile(Engine engine)
    {
        return deserializeRowFromJson(engine, serializedScanFile);
    }
}
