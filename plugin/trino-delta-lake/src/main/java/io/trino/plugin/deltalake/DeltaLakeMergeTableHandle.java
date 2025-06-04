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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DeltaLakeMergeTableHandle(
        DeltaLakeTableHandle tableHandle,
        DeltaLakeInsertTableHandle insertTableHandle,
        Map<String, DeletionVectorEntry> deletionVectors,
        Optional<String> shallowCloneSourceTableLocation)
        implements ConnectorMergeTableHandle
{
    public DeltaLakeMergeTableHandle
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(insertTableHandle, "insertTableHandle is null");
        deletionVectors = ImmutableMap.copyOf(requireNonNull(deletionVectors, "deletionVectors is null"));
        requireNonNull(shallowCloneSourceTableLocation, "shallowCloneSourceTableLocation is null");
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle();
    }
}
