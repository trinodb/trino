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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import static java.util.Objects.requireNonNull;

public class IcebergMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final IcebergTableHandle tableHandle;
    private final IcebergWritableTableHandle insertTableHandle;

    @JsonCreator
    public IcebergMergeTableHandle(IcebergTableHandle tableHandle, IcebergWritableTableHandle insertTableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.insertTableHandle = requireNonNull(insertTableHandle, "insertTableHandle is null");
    }

    @Override
    @JsonProperty
    public IcebergTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public IcebergWritableTableHandle getInsertTableHandle()
    {
        return insertTableHandle;
    }
}
