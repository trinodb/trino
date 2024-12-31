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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

/**
 * Trino {@link ConnectorMergeTableHandle}.
 */
public class PaimonMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final PaimonTableHandle tableHandle;

    @JsonCreator
    public PaimonMergeTableHandle(@JsonProperty("tableHandle") PaimonTableHandle tableHandle)
    {
        this.tableHandle = tableHandle;
    }

    @Override
    @JsonProperty
    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
