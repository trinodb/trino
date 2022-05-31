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
package io.trino.plugin.iceberg.delete;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergSplit;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

/**
 * A Jackson-serializable implementation of the Iceberg {@link DeleteFile}. Allows for delete files to be included in {@link IcebergSplit}.
 */
public class TrinoDeleteFile
        implements DeleteFile
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(TrinoDeleteFile.class).instanceSize();

    @Nullable private final Long pos;
    private final int specId;
    private final FileContent fileContent;
    private final String path;
    private final FileFormat format;
    private final long recordCount;
    private final long fileSizeInBytes;
    @Nullable private final Map<Integer, Long> columnSizes;
    @Nullable private final Map<Integer, Long> valueCounts;
    @Nullable private final Map<Integer, Long> nullValueCounts;
    @Nullable private final Map<Integer, Long> nanValueCounts;
    @Nullable private final Map<Integer, byte[]> lowerBounds;
    @Nullable private final Map<Integer, byte[]> upperBounds;
    @Nullable private final byte[] keyMetadata;
    @Nullable private final List<Integer> equalityFieldIds;
    @Nullable private final Integer sortOrderId;
    @Nullable private final List<Long> splitOffsets;

    public static TrinoDeleteFile copyOf(DeleteFile deleteFile)
    {
        return new TrinoDeleteFile(
                deleteFile.pos(),
                deleteFile.specId(),
                deleteFile.content(),
                deleteFile.path().toString(),
                deleteFile.format(),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                deleteFile.columnSizes(),
                deleteFile.valueCounts(),
                deleteFile.nullValueCounts(),
                deleteFile.nanValueCounts(),
                deleteFile.lowerBounds() == null ? null : deleteFile.lowerBounds().entrySet().stream()
                        .collect(toImmutableMap(Map.Entry<Integer, ByteBuffer>::getKey, entry -> entry.getValue().array())),
                deleteFile.upperBounds() == null ? null : deleteFile.upperBounds().entrySet().stream()
                        .collect(toImmutableMap(Map.Entry<Integer, ByteBuffer>::getKey, entry -> entry.getValue().array())),
                deleteFile.keyMetadata() == null ? null : deleteFile.keyMetadata().array(),
                deleteFile.equalityFieldIds(),
                deleteFile.sortOrderId(),
                deleteFile.splitOffsets());
    }

    @JsonCreator
    public TrinoDeleteFile(
            @JsonProperty("pos") @Nullable Long pos,
            @JsonProperty("specId") int specId,
            @JsonProperty("fileContent") FileContent fileContent,
            @JsonProperty("path") String path,
            @JsonProperty("format") FileFormat format,
            @JsonProperty("recordCount") long recordCount,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("columnSizes") @Nullable Map<Integer, Long> columnSizes,
            @JsonProperty("valueCounts") @Nullable Map<Integer, Long> valueCounts,
            @JsonProperty("nullValueCounts") @Nullable Map<Integer, Long> nullValueCounts,
            @JsonProperty("nanValueCounts") @Nullable Map<Integer, Long> nanValueCounts,
            @JsonProperty("lowerBounds") @Nullable Map<Integer, byte[]> lowerBounds,
            @JsonProperty("upperBounds") @Nullable Map<Integer, byte[]> upperBounds,
            @JsonProperty("keyMetadata") @Nullable byte[] keyMetadata,
            @JsonProperty("equalityFieldIds") @Nullable List<Integer> equalityFieldIds,
            @JsonProperty("sortOrderId") @Nullable Integer sortOrderId,
            @JsonProperty("splitOffsets") @Nullable List<Long> splitOffsets)
    {
        this.pos = pos;
        this.specId = specId;
        this.fileContent = requireNonNull(fileContent, "fileContent is null");
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.columnSizes = columnSizes == null ? null : ImmutableMap.copyOf(columnSizes);
        this.valueCounts = valueCounts == null ? null : ImmutableMap.copyOf(valueCounts);
        this.nullValueCounts = nullValueCounts == null ? null : ImmutableMap.copyOf(nullValueCounts);
        this.nanValueCounts = nanValueCounts == null ? null : ImmutableMap.copyOf(nanValueCounts);
        this.lowerBounds = lowerBounds == null ? null : ImmutableMap.copyOf(lowerBounds);
        this.upperBounds = upperBounds == null ? null : ImmutableMap.copyOf(upperBounds);
        this.keyMetadata = keyMetadata == null ? null : keyMetadata.clone();
        this.equalityFieldIds = equalityFieldIds == null ? null : ImmutableList.copyOf(equalityFieldIds);
        this.sortOrderId = sortOrderId;
        this.splitOffsets = splitOffsets == null ? null : ImmutableList.copyOf(splitOffsets);
    }

    @Override
    @JsonProperty("pos")
    @Nullable
    public Long pos()
    {
        return pos;
    }

    @Override
    @JsonProperty("specId")
    public int specId()
    {
        return specId;
    }

    @Override
    @JsonProperty("fileContent")
    public FileContent content()
    {
        return fileContent;
    }

    @Override
    @JsonProperty("path")
    public CharSequence path()
    {
        return path;
    }

    @Override
    @JsonProperty("format")
    public FileFormat format()
    {
        return format;
    }

    // TODO: Probably need to figure out how to serialize this
    @Override
    @JsonIgnore
    public StructLike partition()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @JsonProperty("recordCount")
    public long recordCount()
    {
        return recordCount;
    }

    @Override
    @JsonProperty("fileSizeInBytes")
    public long fileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @Override
    @JsonProperty("columnSizes")
    @Nullable
    public Map<Integer, Long> columnSizes()
    {
        return columnSizes;
    }

    @Override
    @JsonProperty("valueCounts")
    @Nullable
    public Map<Integer, Long> valueCounts()
    {
        return valueCounts;
    }

    @Override
    @JsonProperty("nullValueCounts")
    @Nullable
    public Map<Integer, Long> nullValueCounts()
    {
        return nullValueCounts;
    }

    @Override
    @JsonProperty("nanValueCounts")
    @Nullable
    public Map<Integer, Long> nanValueCounts()
    {
        return nanValueCounts;
    }

    @JsonProperty("lowerBounds")
    @Nullable
    public Map<Integer, byte[]> lowerBoundsAsByteArray()
    {
        return lowerBounds;
    }

    @Override
    @Nullable
    public Map<Integer, ByteBuffer> lowerBounds()
    {
        if (lowerBounds == null) {
            return null;
        }
        return lowerBounds.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ByteBuffer.wrap(entry.getValue())));
    }

    @JsonProperty("upperBounds")
    @Nullable
    public Map<Integer, byte[]> upperBoundsAsByteArray()
    {
        return upperBounds;
    }

    @Override
    @Nullable
    public Map<Integer, ByteBuffer> upperBounds()
    {
        if (upperBounds == null) {
            return null;
        }
        return upperBounds.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ByteBuffer.wrap(entry.getValue())));
    }

    @JsonProperty("keyMetadata")
    @Nullable
    public byte[] keyMetadataAsByteArray()
    {
        return keyMetadata;
    }

    @Override
    @Nullable
    public ByteBuffer keyMetadata()
    {
        if (keyMetadata == null) {
            return null;
        }
        return ByteBuffer.wrap(keyMetadata);
    }

    @Override
    @JsonProperty("equalityFieldIds")
    @Nullable
    public List<Integer> equalityFieldIds()
    {
        return equalityFieldIds;
    }

    @Override
    @JsonProperty("sortOrderId")
    @Nullable
    public Integer sortOrderId()
    {
        return sortOrderId;
    }

    @Override
    @JsonProperty("splitOffsets")
    @Nullable
    public List<Long> splitOffsets()
    {
        return splitOffsets;
    }

    @Override
    public DeleteFile copy()
    {
        return this;
    }

    @Override
    public DeleteFile copyWithoutStats()
    {
        return new TrinoDeleteFile(
                pos,
                specId,
                fileContent,
                path,
                format,
                recordCount,
                fileSizeInBytes,
                null,
                null,
                null,
                null,
                null,
                null,
                keyMetadata,
                equalityFieldIds,
                sortOrderId,
                splitOffsets);
    }

    public long getRetainedSizeInBytes()
    {
        ToLongFunction<Integer> intSizeOf = ignored -> SIZE_OF_INT;
        ToLongFunction<Long> longSizeOf = ignored -> SIZE_OF_LONG;
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(columnSizes, intSizeOf, longSizeOf)
                + estimatedSizeOf(nullValueCounts, intSizeOf, longSizeOf)
                + estimatedSizeOf(nanValueCounts, intSizeOf, longSizeOf)
                + estimatedSizeOf(lowerBounds, intSizeOf, value -> value.length)
                + estimatedSizeOf(upperBounds, intSizeOf, value -> value.length)
                + (keyMetadata == null ? 0 : keyMetadata.length)
                + estimatedSizeOf(equalityFieldIds, intSizeOf)
                + estimatedSizeOf(splitOffsets, longSizeOf);
    }
}
