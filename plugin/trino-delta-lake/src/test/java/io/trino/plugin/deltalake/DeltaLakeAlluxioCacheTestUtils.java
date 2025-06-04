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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.testing.QueryRunner;

import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.CacheOperation;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getCacheOperationSpans;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_POSITION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_SIZE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public final class DeltaLakeAlluxioCacheTestUtils
{
    private static final Pattern dataFilePattern = Pattern.compile(".*?/(?<partition>((\\w+)=[^/]*/)*)(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private DeltaLakeAlluxioCacheTestUtils()
    {}

    public static Multiset<CacheOperation> getCacheOperations(QueryRunner queryRunner)
    {
        return getCacheOperationSpans(queryRunner)
                .stream()
                .map(DeltaLakeAlluxioCacheTestUtils::createCacheOperation)
                .collect(toCollection(HashMultiset::create));
    }

    private static CacheOperation createCacheOperation(SpanData span)
    {
        String operationName = span.getName();
        Attributes attributes = span.getAttributes();
        String path = getFileLocation(span);
        String fileName = path.replaceFirst(".*/", "");

        OptionalLong position = switch (operationName) {
            case "Alluxio.readCached", "Alluxio.readExternalStream" -> OptionalLong.of(requireNonNull(attributes.get(CACHE_FILE_READ_POSITION)));
            case "Alluxio.writeCache" -> OptionalLong.of(requireNonNull(attributes.get(CACHE_FILE_WRITE_POSITION)));
            case "Input.readFully" -> OptionalLong.of(requireNonNull(attributes.get(FILE_READ_POSITION)));
            default -> OptionalLong.empty();
        };

        OptionalLong length = switch (operationName) {
            case "Alluxio.readCached", "Alluxio.readExternalStream" -> OptionalLong.of(requireNonNull(attributes.get(CACHE_FILE_READ_SIZE)));
            case "Alluxio.writeCache" -> OptionalLong.of(requireNonNull(attributes.get(CACHE_FILE_WRITE_SIZE)));
            case "Input.readFully" -> OptionalLong.of(requireNonNull(attributes.get(FILE_READ_SIZE)));
            default -> OptionalLong.empty();
        };

        if (!path.contains("_delta_log") && !path.contains("/.trino")) {
            Matcher matcher = dataFilePattern.matcher(path);
            if (matcher.matches()) {
                String changeData = path.contains("/_change_data/") ? "change_data/" : "";
                if (!path.contains("=")) {
                    return new CacheOperation(operationName, "data", position, length);
                }
                return new CacheOperation(operationName, changeData + matcher.group("partition"), position, length);
            }
            if (path.contains("/part-00000-")) {
                return new CacheOperation(operationName, "data", position, length);
            }
            if (path.contains("/deletion_vector_")) {
                return new CacheOperation(operationName, "deletion_vector", position, length);
            }
        }
        else {
            return new CacheOperation(operationName, fileName, position, length);
        }
        throw new IllegalArgumentException("File not recognized: " + path);
    }
}
