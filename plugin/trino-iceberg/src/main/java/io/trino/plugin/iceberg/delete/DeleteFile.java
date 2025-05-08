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

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.types.Conversions;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public record DeleteFile(
        FileContent content,
        String path,
        FileFormat format,
        long recordCount,
        long fileSizeInBytes,
        List<Integer> equalityFieldIds,
        Optional<Long> rowPositionLowerBound,
        Optional<Long> rowPositionUpperBound,
        long dataSequenceNumber)
{
    private static final long INSTANCE_SIZE = instanceSize(DeleteFile.class);

    public static DeleteFile fromIceberg(org.apache.iceberg.DeleteFile deleteFile)
    {
        Optional<Long> rowPositionLowerBound = Optional.ofNullable(deleteFile.lowerBounds())
                .map(bounds -> bounds.get(DELETE_FILE_POS.fieldId()))
                .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), bytes));
        Optional<Long> rowPositionUpperBound = Optional.ofNullable(deleteFile.upperBounds())
                .map(bounds -> bounds.get(DELETE_FILE_POS.fieldId()))
                .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), bytes));

        return new DeleteFile(
                deleteFile.content(),
                deleteFile.location(),
                deleteFile.format(),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                Optional.ofNullable(deleteFile.equalityFieldIds()).orElseGet(ImmutableList::of),
                rowPositionLowerBound,
                rowPositionUpperBound,
                deleteFile.dataSequenceNumber());
    }

    public DeleteFile
    {
        requireNonNull(content, "content is null");
        requireNonNull(path, "path is null");
        requireNonNull(format, "format is null");
        equalityFieldIds = ImmutableList.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
        requireNonNull(rowPositionLowerBound, "rowPositionLowerBound is null");
        requireNonNull(rowPositionUpperBound, "rowPositionUpperBound is null");
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(equalityFieldIds, _ -> SIZE_OF_INT);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .add("records", recordCount)
                .toString();
    }
}
