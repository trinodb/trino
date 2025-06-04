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
package io.trino.filesystem.tracing;

import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static java.util.Objects.requireNonNull;

public class CacheFileSystemTraceUtils
{
    private CacheFileSystemTraceUtils() {}

    public record CacheOperation(String operationName, String fileId, OptionalLong position, OptionalLong length)
    {
        public CacheOperation(String operationName, String fileId, long position, long length)
        {
            this(operationName, fileId, OptionalLong.of(position), OptionalLong.of(length));
        }

        public CacheOperation(String operationName, String fileId)
        {
            this(operationName, fileId, OptionalLong.empty(), OptionalLong.empty());
        }
    }

    public static List<SpanData> getCacheOperationSpans(QueryRunner queryRunner)
    {
        return queryRunner.getSpans().stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.") || span.getName().startsWith("Alluxio."))
                .filter(span -> !span.getName().startsWith("InputFile.newInput"))
                .filter(span -> !isTrinoSchemaOrPermissions(getFileLocation(span)))
                .collect(toImmutableList());
    }

    public static String getFileLocation(SpanData span)
    {
        if (span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.")) {
            return requireNonNull(span.getAttributes().get(FILE_LOCATION));
        }
        return requireNonNull(span.getAttributes().get(CACHE_FILE_LOCATION));
    }

    public static boolean isTrinoSchemaOrPermissions(String path)
    {
        return path.endsWith(".trinoSchema") || path.contains(".trinoPermissions") || path.contains(".roleGrants") || path.contains(".roles");
    }
}
