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
package io.trino.plugin.hudi.util;

import io.opentelemetry.sdk.trace.data.SpanData;

import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static java.util.Objects.requireNonNull;

public final class FileOperationUtils
{
    private FileOperationUtils() {}

    public record FileOperation(String operationType, FileType fileType)
    {
        public FileOperation
        {
            requireNonNull(operationType, "operationType is null");
            requireNonNull(fileType, "fileType is null");
        }

        public static FileOperation create(SpanData span)
        {
            String path = getFileLocation(span);
            return new FileOperation(span.getName(), FileOperationUtils.FileType.fromFilePath(path));
        }
    }

    public enum FileType
    {
        METADATA_TABLE_PROPERTIES,
        METADATA_TABLE,
        TIMELINE,
        TABLE_PROPERTIES,
        INDEX_DEFINITION,
        OTHER_TABLE_METADATA,
        COMMIT_INFLIGHT,
        DATA,
        LOG,
        METASTORE;

        public static FileType fromFilePath(String path)
        {
            if (path.contains("/.hoodie")) {
                if (path.endsWith("/.hoodie/metadata/.hoodie/hoodie.properties")) {
                    return METADATA_TABLE_PROPERTIES;
                }
                if (path.contains("/.hoodie/metadata")) {
                    return METADATA_TABLE;
                }
                if (path.contains("/.hoodie/timeline")) {
                    return TIMELINE;
                }
                if (path.endsWith("/.hoodie/hoodie.properties")) {
                    return TABLE_PROPERTIES;
                }
                if (path.endsWith("/.hoodie/.index_defs/index.json")) {
                    return INDEX_DEFINITION;
                }
                return OTHER_TABLE_METADATA;
            }
            if (path.endsWith(".parquet")) {
                return DATA;
            }
            if (path.contains(".log")) {
                return LOG;
            }
            if (path.endsWith(".trinoSchema") || path.contains("/.trinoPermissions/")) {
                return METASTORE;
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }
}
