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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import static java.util.Objects.requireNonNull;

public class HiveMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final HiveTableHandle tableHandle;
    private final HiveInsertTableHandle insertHandle;

    @JsonCreator
    public HiveMergeTableHandle(
            @JsonProperty("tableHandle") HiveTableHandle tableHandle,
            @JsonProperty("insertHandle") HiveInsertTableHandle insertHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.insertHandle = requireNonNull(insertHandle, "insertHandle is null");
    }

    @Override
    @JsonProperty
    public HiveTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public HiveInsertTableHandle getInsertHandle()
    {
        return insertHandle;
    }
}
