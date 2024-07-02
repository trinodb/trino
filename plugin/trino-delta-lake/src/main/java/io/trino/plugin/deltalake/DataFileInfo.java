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

import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DataFileInfo(
        String path,
        long size,
        long creationTime,
        io.trino.plugin.deltalake.DataFileInfo.DataFileType dataFileType,
        List<String> partitionValues,
        DeltaLakeJsonFileStatistics statistics,
        Optional<DeletionVectorEntry> deletionVector)
{
    public enum DataFileType
    {
        DATA,
        CHANGE_DATA_FEED,
    }

    public DataFileInfo
    {
        requireNonNull(dataFileType, "dataFileType is null");
        requireNonNull(statistics, "statistics is null");
        requireNonNull(deletionVector, "deletionVector is null");
    }
}
