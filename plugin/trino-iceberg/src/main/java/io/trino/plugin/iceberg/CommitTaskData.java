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

import org.apache.iceberg.FileContent;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record CommitTaskData(
        String path,
        IcebergFileFormat fileFormat,
        long fileSizeInBytes,
        MetricsWrapper metrics,
        String partitionSpecJson,
        Optional<String> partitionDataJson,
        FileContent content,
        Optional<String> referencedDataFile)
{
    public CommitTaskData
    {
        requireNonNull(path, "path is null");
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(metrics, "metrics is null");
        requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        requireNonNull(partitionDataJson, "partitionDataJson is null");
        requireNonNull(content, "content is null");
        requireNonNull(referencedDataFile, "referencedDataFile is null");
    }
}
