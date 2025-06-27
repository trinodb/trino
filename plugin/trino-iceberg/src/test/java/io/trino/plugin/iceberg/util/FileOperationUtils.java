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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.trace.data.SpanData;

import java.util.List;
import java.util.function.Predicate;

import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METASTORE;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.fromFilePath;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public final class FileOperationUtils
{
    private FileOperationUtils() {}

    public static Multiset<FileOperation> getOperations(List<SpanData> spans)
    {
        return spans.stream()
                .filter(span -> span.getName().startsWith("InputFile.") || span.getName().startsWith("OutputFile."))
                .map(span -> new FileOperation(fromFilePath(requireNonNull(span.getAttributes().get(FILE_LOCATION))), span.getName()))
                .collect(toCollection(HashMultiset::create));
    }

    public record FileOperation(FileType fileType, String operationType)
    {
        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    public enum Scope
            implements Predicate<FileOperation>
    {
        METADATA_FILES {
            @Override
            public boolean test(FileOperation fileOperation)
            {
                return fileOperation.fileType() != DATA && fileOperation.fileType() != METASTORE;
            }
        },
        ALL_FILES {
            @Override
            public boolean test(FileOperation fileOperation)
            {
                return fileOperation.fileType() != METASTORE;
            }
        },
    }

    public enum FileType
    {
        METADATA_JSON,
        SNAPSHOT,
        MANIFEST,
        STATS,
        DATA,
        DELETE,
        METASTORE,
        /**/;

        public static FileType fromFilePath(String path)
        {
            if (path.endsWith("metadata.json")) {
                return METADATA_JSON;
            }
            if (path.contains("/snap-")) {
                return SNAPSHOT;
            }
            if (path.endsWith("-m0.avro")) {
                return MANIFEST;
            }
            if (path.endsWith(".stats")) {
                return STATS;
            }
            if (path.contains("/data/") && (path.endsWith(".orc") || path.endsWith(".parquet"))) {
                return DATA;
            }
            if (path.contains("delete_file") && (path.endsWith(".orc") || path.endsWith(".parquet"))) {
                return DELETE;
            }
            if (path.endsWith(".trinoSchema") || path.contains("/.trinoPermissions/")) {
                return METASTORE;
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }
}
