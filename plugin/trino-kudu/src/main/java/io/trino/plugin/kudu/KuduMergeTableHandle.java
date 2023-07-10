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
package io.trino.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KuduMergeTableHandle
        implements ConnectorMergeTableHandle, KuduTableMapping
{
    private final KuduTableHandle tableHandle;
    private final KuduOutputTableHandle outputTableHandle;

    @JsonCreator
    public KuduMergeTableHandle(
            @JsonProperty("tableHandle") KuduTableHandle tableHandle,
            @JsonProperty("outputTableHandle") KuduOutputTableHandle outputTableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.outputTableHandle = requireNonNull(outputTableHandle, "outputTableHandle is null");
    }

    @JsonProperty
    @Override
    public KuduTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public KuduOutputTableHandle getOutputTableHandle()
    {
        return outputTableHandle;
    }

    @Override
    public boolean isGenerateUUID()
    {
        return outputTableHandle.isGenerateUUID();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return outputTableHandle.getColumnTypes();
    }

    @Override
    public List<Type> getOriginalColumnTypes()
    {
        return outputTableHandle.getOriginalColumnTypes();
    }
}
