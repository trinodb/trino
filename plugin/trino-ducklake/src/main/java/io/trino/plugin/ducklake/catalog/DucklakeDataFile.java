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
package io.trino.plugin.ducklake.catalog;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents a data file from the ducklake_data_file table.
 */
public record DucklakeDataFile(
        long dataFileId,
        long tableId,
        long beginSnapshot,
        Optional<Long> endSnapshot,
        long fileOrder,
        String path,
        boolean pathIsRelative,
        String fileFormat,
        long recordCount,
        long fileSizeBytes,
        long footerSize,
        long rowIdStart,
        Optional<String> deleteFilePath,
        Optional<Boolean> deleteFilePathIsRelative)
{
    public DucklakeDataFile
    {
        requireNonNull(path, "path is null");
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(endSnapshot, "endSnapshot is null");
        requireNonNull(deleteFilePath, "deleteFilePath is null");
        requireNonNull(deleteFilePathIsRelative, "deleteFilePathIsRelative is null");
    }
}
