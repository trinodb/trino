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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record IcebergInputInfo(
        int formatVersion,
        Optional<Long> snapshotId,
        List<String> partitionFields,
        String tableDefaultFileFormat,
        Optional<String> totalRecords,
        Optional<String> deletedRecords,
        Optional<String> totalDataFiles,
        Optional<String> totalDeleteFiles)
{
    public IcebergInputInfo
    {
        requireNonNull(snapshotId, "snapshotId is null");
        partitionFields = ImmutableList.copyOf(partitionFields);
        requireNonNull(tableDefaultFileFormat, "tableDefaultFileFormat is null");
        requireNonNull(totalRecords, "totalRecords is null");
        requireNonNull(deletedRecords, "deletedRecords is null");
        requireNonNull(totalDataFiles, "totalDataFiles is null");
        requireNonNull(totalDeleteFiles, "totalDeleteFiles is null");
    }
}
