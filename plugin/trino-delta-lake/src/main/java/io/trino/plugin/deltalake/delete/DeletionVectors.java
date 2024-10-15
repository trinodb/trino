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
package io.trino.plugin.deltalake.delete;

import com.google.common.base.CharMatcher;
import io.delta.kernel.internal.deletionvectors.Base85Codec;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.spi.TrinoException;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.delta.kernel.internal.deletionvectors.Base85Codec.decodeUUID;
import static io.delta.kernel.internal.deletionvectors.Base85Codec.encodeUUID;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.UUID.randomUUID;

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format
public final class DeletionVectors
{
    private static final int PORTABLE_ROARING_BITMAP_MAGIC_NUMBER = 1681511377;
    private static final int MAGIC_NUMBER_BYTE_SIZE = 4;
    private static final int BIT_MAP_COUNT_BYTE_SIZE = 8;
    private static final int BIT_MAP_KEY_BYTE_SIZE = 4;
    private static final int FORMAT_VERSION_V1 = 1;

    private static final String UUID_MARKER = "u"; // relative path with random prefix on disk
    private static final String PATH_MARKER = "p"; // absolute path on disk
    private static final String INLINE_MARKER = "i"; // inline

    private static final CharMatcher ALPHANUMERIC = CharMatcher.inRange('A', 'Z').or(CharMatcher.inRange('a', 'z')).or(CharMatcher.inRange('0', '9')).precomputed();

    private DeletionVectors() {}

    public static RoaringBitmapArray readDeletionVectors(TrinoFileSystem fileSystem, Location location, DeletionVectorEntry deletionVector)
            throws IOException
    {
        if (deletionVector.storageType().equals(UUID_MARKER)) {
            TrinoInputFile inputFile = fileSystem.newInputFile(location.appendPath(toFileName(deletionVector.pathOrInlineDv())));
            ByteBuffer buffer = readDeletionVector(inputFile, deletionVector.offset().orElseThrow(), deletionVector.sizeInBytes());
            return deserializeDeletionVectors(buffer);
        }
        if (deletionVector.storageType().equals(INLINE_MARKER) || deletionVector.storageType().equals(PATH_MARKER)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported storage type for deletion vector: " + deletionVector.storageType());
        }
        throw new IllegalArgumentException("Unexpected storage type: " + deletionVector.storageType());
    }

    public static DeletionVectorEntry writeDeletionVectors(
            TrinoFileSystem fileSystem,
            Location location,
            RoaringBitmapArray deletedRows)
            throws IOException
    {
        UUID uuid = randomUUID();
        String deletionVectorFilename = "deletion_vector_" + uuid + ".bin";
        String pathOrInlineDv = encodeUUID(uuid);
        int sizeInBytes = MAGIC_NUMBER_BYTE_SIZE + BIT_MAP_COUNT_BYTE_SIZE + BIT_MAP_KEY_BYTE_SIZE + deletedRows.serializedSizeInBytes();
        long cardinality = deletedRows.cardinality();

        checkArgument(sizeInBytes > 0, "sizeInBytes must be positive: %s", sizeInBytes);
        checkArgument(cardinality > 0, "cardinality must be positive: %s", cardinality);

        OptionalInt offset;
        byte[] data = serializeAsByteArray(deletedRows, sizeInBytes);
        try (DataOutputStream output = new DataOutputStream(fileSystem.newOutputFile(location.appendPath(deletionVectorFilename)).create())) {
            output.writeByte(FORMAT_VERSION_V1);
            offset = OptionalInt.of(output.size());
            output.writeInt(sizeInBytes);
            output.write(data);
            output.writeInt(calculateChecksum(data));
        }

        return new DeletionVectorEntry(UUID_MARKER, pathOrInlineDv, offset, sizeInBytes, cardinality);
    }

    private static byte[] serializeAsByteArray(RoaringBitmapArray bitmaps, int sizeInBytes)
    {
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes).order(LITTLE_ENDIAN);
        buffer.putInt(PORTABLE_ROARING_BITMAP_MAGIC_NUMBER);
        buffer.putLong(bitmaps.length());
        for (int i = 0; i < bitmaps.length(); i++) {
            buffer.putInt(i); // Bitmap index
            RoaringBitmap bitmap = bitmaps.get(i);
            bitmap.runOptimize();
            bitmap.serialize(buffer);
        }
        return buffer.array();
    }

    public static String toFileName(String pathOrInlineDv)
    {
        int randomPrefixLength = pathOrInlineDv.length() - Base85Codec.ENCODED_UUID_LENGTH;
        String randomPrefix = pathOrInlineDv.substring(0, randomPrefixLength);
        checkArgument(ALPHANUMERIC.matchesAllOf(randomPrefix), "Random prefix must be alphanumeric: %s", randomPrefix);
        String prefix = randomPrefix.isEmpty() ? "" : randomPrefix + "/";
        String encodedUuid = pathOrInlineDv.substring(randomPrefixLength);
        UUID uuid = decodeUUID(encodedUuid);
        return "%sdeletion_vector_%s.bin".formatted(prefix, uuid);
    }

    public static ByteBuffer readDeletionVector(TrinoInputFile inputFile, int offset, int expectedSize)
            throws IOException
    {
        try (TrinoInput input = inputFile.newInput()) {
            ByteBuffer buffer = input.readFully(offset, SIZE_OF_INT + expectedSize + SIZE_OF_INT).toByteBuffer();
            int actualSize = buffer.getInt(0);
            if (actualSize != expectedSize) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "The size of deletion vector %s expects %s but got %s".formatted(inputFile.location(), expectedSize, actualSize));
            }
            int checksum = buffer.getInt(SIZE_OF_INT + expectedSize);
            if (calculateChecksum(buffer.array(), buffer.arrayOffset() + SIZE_OF_INT, expectedSize) != checksum) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Checksum mismatch for deletion vector: " + inputFile.location());
            }
            return buffer.slice(SIZE_OF_INT, expectedSize).order(LITTLE_ENDIAN);
        }
    }

    private static int calculateChecksum(byte[] data)
    {
        return calculateChecksum(data, 0, data.length);
    }

    private static int calculateChecksum(byte[] data, int offset, int length)
    {
        // Delta Lake allows integer overflow intentionally because it's fine from checksum perspective
        // https://github.com/delta-io/delta/blob/039a29abb4abc72ac5912651679233dc983398d6/spark/src/main/scala/org/apache/spark/sql/delta/storage/dv/DeletionVectorStore.scala#L115
        Checksum crc = new CRC32();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    private static RoaringBitmapArray deserializeDeletionVectors(ByteBuffer buffer)
            throws IOException
    {
        checkArgument(buffer.order() == LITTLE_ENDIAN, "Byte order must be little endian: %s", buffer.order());
        int magicNumber = buffer.getInt();
        if (magicNumber == PORTABLE_ROARING_BITMAP_MAGIC_NUMBER) {
            int size = toIntExact(buffer.getLong());
            RoaringBitmapArray bitmaps = new RoaringBitmapArray();
            for (int i = 0; i < size; i++) {
                int key = buffer.getInt();
                checkArgument(key >= 0, "key must not be negative: %s", key);

                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(buffer);
                bitmap.stream().forEach(bitmaps::add);

                // there seems to be no better way to ask how many bytes bitmap.deserialize has read
                int consumedBytes = bitmap.serializedSizeInBytes();
                buffer.position(buffer.position() + consumedBytes);
            }
            return bitmaps;
        }
        throw new IllegalArgumentException("Unsupported magic number: " + magicNumber);
    }
}
