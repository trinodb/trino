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
package io.trino.plugin.raptor.legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import static java.util.Objects.requireNonNull;

public class RaptorMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final RaptorTableHandle tableHandle;
    private final RaptorInsertTableHandle insertTableHandle;

    @JsonCreator
    public RaptorMergeTableHandle(
            @JsonProperty RaptorTableHandle tableHandle,
            @JsonProperty RaptorInsertTableHandle insertTableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.insertTableHandle = requireNonNull(insertTableHandle, "insertTableHandle is null");
    }

    @Override
    @JsonProperty
    public RaptorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public RaptorInsertTableHandle getInsertTableHandle()
    {
        return insertTableHandle;
    }
}
