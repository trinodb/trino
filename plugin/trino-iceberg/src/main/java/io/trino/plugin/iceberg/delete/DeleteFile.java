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
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.types.Conversions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public record DeleteFile(
        FileContent content,
        String path,
        FileFormat format,
        long recordCount,
        long fileSizeInBytes,
        List<Integer> equalityFieldIds,
        OptionalLong rowPositionLowerBound,
        OptionalLong rowPositionUpperBound,
        long dataSequenceNumber,
        OptionalLong contentOffset,
        Optional<Integer> contentSizeInBytes,
        Optional<String> referencedDataFile)
{
    private static final long INSTANCE_SIZE = instanceSize(DeleteFile.class);

    public static DeleteFile fromIceberg(org.apache.iceberg.DeleteFile deleteFile)
    {
        ByteBuffer lowerBoundPosition = requireNonNullElse(deleteFile.lowerBounds(), ImmutableMap.<Integer, ByteBuffer>of()).get(DELETE_FILE_POS.fieldId());
        ByteBuffer upperBoundPosition = requireNonNullElse(deleteFile.upperBounds(), ImmutableMap.<Integer, ByteBuffer>of()).get(DELETE_FILE_POS.fieldId());

        OptionalLong rowPositionLowerBound = lowerBoundPosition == null ?
                OptionalLong.empty() : OptionalLong.of(Conversions.fromByteBuffer(DELETE_FILE_POS.type(), lowerBoundPosition));

        OptionalLong rowPositionUpperBound = upperBoundPosition == null ?
                OptionalLong.empty() : OptionalLong.of(Conversions.fromByteBuffer(DELETE_FILE_POS.type(), upperBoundPosition));

        OptionalLong contentOffset = deleteFile.contentOffset() == null ? OptionalLong.empty() : OptionalLong.of(deleteFile.contentOffset());
        Optional<Integer> contentSizeInBytes = Optional.ofNullable(deleteFile.contentSizeInBytes()).map(Math::toIntExact);

        // Iceberg's DeleteFile declares dataSequenceNumber() as boxed Long. The production
        // manifest-read path always assigns it before handing the file to Trino, but in-memory
        // builders leave it null. Guard explicitly so auto-unboxing cannot turn a misuse into a
        // bare NPE that hides which field was missing.
        Long dataSequenceNumber = requireNonNull(
                deleteFile.dataSequenceNumber(),
                () -> "Iceberg DeleteFile is missing dataSequenceNumber: " + deleteFile.location());

        return new DeleteFile(
                deleteFile.content(),
                deleteFile.location(),
                deleteFile.format(),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                Optional.ofNullable(deleteFile.equalityFieldIds()).orElseGet(ImmutableList::of),
                rowPositionLowerBound,
                rowPositionUpperBound,
                dataSequenceNumber,
                contentOffset,
                contentSizeInBytes,
                Optional.ofNullable(deleteFile.referencedDataFile()));
    }

    public DeleteFile
    {
        requireNonNull(content, "content is null");
        requireNonNull(path, "path is null");
        requireNonNull(format, "format is null");
        equalityFieldIds = ImmutableList.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
        requireNonNull(rowPositionLowerBound, "rowPositionLowerBound is null");
        requireNonNull(rowPositionUpperBound, "rowPositionUpperBound is null");
        requireNonNull(contentOffset, "contentOffset is null");
        requireNonNull(contentSizeInBytes, "contentSizeInBytes is null");
        requireNonNull(referencedDataFile, "referencedDataFile is null");
    }

    public boolean isDeletionVector()
    {
        return content == FileContent.POSITION_DELETES
                && format == FileFormat.PUFFIN
                && contentOffset.isPresent()
                && contentSizeInBytes.isPresent();
    }

    // A position delete file is "file-scoped" when it references exactly one data file.
    // When that data file is rewritten (removed), the delete file becomes dangling and
    // must be cleaned up. Partition-scoped deletes (no referencedDataFile) may still apply
    // to sibling data files in the same partition and must NOT be removed.
    public boolean isFileScopedPositionDelete(String dataFilePath)
    {
        return content == FileContent.POSITION_DELETES
                && referencedDataFile.isPresent()
                && referencedDataFile.get().equals(dataFilePath);
    }

    public long retainedSizeInBytes()
    {
        // referencedDataFile is populated for v3 deletion vectors and for file-scoped position
        // delete files. Excluding it under-reports memory for any table on the v3 read path.
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(equalityFieldIds, _ -> SIZE_OF_INT)
                + sizeOf(referencedDataFile, value -> estimatedSizeOf(value));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitEmptyValues()
                .add("format", format)
                .add("path", path)
                .add("offset", contentOffset)
                .add("size", contentSizeInBytes)
                .add("records", recordCount)
                .toString();
    }
}
