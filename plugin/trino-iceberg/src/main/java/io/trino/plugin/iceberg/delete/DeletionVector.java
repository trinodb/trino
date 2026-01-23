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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_DELETION_VECTOR_TOO_LARGE;

public final class DeletionVector
{
    private static final int MAGIC_LE = 0x64_39_d3_d1;
    private static final int LENGTH_SIZE_BYTES = 4;
    private static final int CRC_SIZE_BYTES = 4;

    private final int bitmapCount;
    private final RoaringBitmap[] deletedRows;

    private DeletionVector(int bitmapCount, RoaringBitmap[] deletedRows)
    {
        this.bitmapCount = bitmapCount;
        checkArgument(bitmapCount > 0, "bitmapCount must be positive");
        this.deletedRows = deletedRows;
    }

    public boolean isRowDeleted(long filePos)
    {
        int key = key(filePos);
        if (key >= deletedRows.length) {
            return false;
        }
        RoaringBitmap deletedRow = deletedRows[key];
        return deletedRow != null && deletedRow.contains(low(filePos));
    }

    public void forEachDeletedRow(LongConsumer consumer)
    {
        for (int key = 0; key < deletedRows.length; key++) {
            RoaringBitmap bitmap = deletedRows[key];
            if (bitmap != null && !bitmap.isEmpty()) {
                bitmap.forEach(intToLongAdapter(key, consumer));
            }
        }
    }

    private static IntConsumer intToLongAdapter(int keyHigh, LongConsumer consumer)
    {
        return keyLow -> consumer.accept((((long) keyHigh) << 32) | (keyLow & 0xFFFFFFFFL));
    }

    public Slice serialize()
    {
        // NOTE: the deletion vector randomly mixes big-endian and little-endian integers, so be very careful in this code

        // optimize bitmaps for more compact serialization
        for (RoaringBitmap bitmap : deletedRows) {
            if (bitmap != null) {
                bitmap.runOptimize();
            }
        }

        // calculate bodyLength
        int bodyLength = serializedSizeInBytes();

        // allocate slice
        Slice slice = Slices.allocate(LENGTH_SIZE_BYTES + bodyLength + CRC_SIZE_BYTES);
        int offset = 0;

        // write bodyLength
        slice.setInt(offset, Integer.reverseBytes(bodyLength));
        offset += Integer.BYTES;

        // write magic number
        slice.setInt(offset, MAGIC_LE);
        offset += Integer.BYTES;

        // write bitmaps
        slice.setLong(offset, bitmapCount);
        offset += SIZE_OF_LONG;
        for (int key = 0; key < deletedRows.length; key++) {
            RoaringBitmap bitmap = deletedRows[key];
            if (bitmap != null && !bitmap.isEmpty()) {
                // write key
                slice.setInt(offset, key);
                offset += Integer.BYTES;

                // write bitmap
                int serializedSize = bitmap.serializedSizeInBytes();
                bitmap.serialize(slice.slice(offset, serializedSize).toByteBuffer());
                offset += serializedSize;
            }
        }

        // write checksum
        slice.setInt(offset, Integer.reverseBytes(crc32(slice, LENGTH_SIZE_BYTES, bodyLength)));

        return slice;
    }

    private static int crc32(Slice data, int offset, int length)
    {
        CRC32 crc = new CRC32();
        crc.update(data.byteArray(), offset, length);
        return (int) crc.getValue();
    }

    private int serializedSizeInBytes()
    {
        long size = Integer.BYTES; // magic number
        size += SIZE_OF_LONG; // bitmap count
        for (RoaringBitmap bitmap : deletedRows) {
            if (bitmap != null && !bitmap.isEmpty()) {
                size += Integer.BYTES; // key
                size += bitmap.serializedSizeInBytes(); // bitmap
            }
        }
        long bufferSize = LENGTH_SIZE_BYTES + size + CRC_SIZE_BYTES;
        if (bufferSize > Integer.MAX_VALUE) {
            throw new TrinoException(ICEBERG_DELETION_VECTOR_TOO_LARGE, "Can't serialize deletion vector > 2GB");
        }
        return (int) size;
    }

    public long cardinality()
    {
        long cardinality = 0L;
        for (RoaringBitmap bitmap : deletedRows) {
            if (bitmap != null) {
                cardinality += bitmap.getLongCardinality();
            }
        }
        return cardinality;
    }

    private static int key(long pos)
    {
        return (int) (pos >>> 32);
    }

    private static int low(long pos)
    {
        return (int) pos;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private int bitmapCount;
        private RoaringBitmap[] deletedRows = new RoaringBitmap[0];

        public Builder deserialize(Slice data)
        {
            // NOTE: the deletion vector randomly mixes big-endian and little-endian integers, so be very careful in this code

            int offset = 0;

            // read and validate length
            int length = Integer.reverseBytes(data.getInt(offset));
            offset += Integer.BYTES;
            long expectedLength = data.length() - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
            checkArgument(length == expectedLength, "Invalid bitmap data length: %s, expected %s", length, expectedLength);

            // verify checksum before processing data
            int crcOffset = LENGTH_SIZE_BYTES + length;
            checkArgument(crc32(data, LENGTH_SIZE_BYTES, length) == Integer.reverseBytes(data.getInt(crcOffset)), "Invalid CRC");

            // read and validate magic number
            int magicNumber = data.getInt(offset);
            offset += Integer.BYTES;
            checkArgument(magicNumber == MAGIC_LE, "Invalid magic number: %s", magicNumber);

            // read bitmaps
            long bitmapCount = data.getLong(offset);
            offset += SIZE_OF_LONG;
            checkArgument(bitmapCount >= 0 && bitmapCount <= Integer.MAX_VALUE, "Invalid bitmap count: %s", bitmapCount);
            int lastKey = -1;
            for (int i = 0; i < bitmapCount; i++) {
                // bitmaps are stored as key-value pairs where the
                // key is the high 32 bits of the file position
                // and the value is a roaring bitmap of low 32 bits
                int key = data.getInt(offset);
                offset += Integer.BYTES;

                checkArgument(key >= 0, "Invalid unsigned key: %s", key);
                checkArgument(key <= Integer.MAX_VALUE - 1, "Key is too large: %s", key);
                checkArgument(key > lastKey, "Keys must be sorted in ascending order");

                RoaringBitmap bitmap = new RoaringBitmap();
                try {
                    bitmap.deserialize(data.slice(offset, data.length() - offset).toByteBuffer().asReadOnlyBuffer());
                }
                catch (IOException e) {
                    throw new TrinoException(ICEBERG_BAD_DATA, "Failed to deserialize deletion vector bitmap for key: " + key, e);
                }
                offset += bitmap.serializedSizeInBytes();
                checkArgument(offset <= crcOffset, "Bitmap deserialization read past data boundary");

                RoaringBitmap existing = getOrCreateBitmap(key);
                existing.or(bitmap);

                lastKey = key;
            }

            return this;
        }

        public Builder add(long pos)
        {
            getOrCreateBitmap(key(pos)).add(low(pos));
            return this;
        }

        public Builder addAll(DeletionVector deletionVector)
        {
            return addAll(deletionVector.deletedRows);
        }

        public Builder addAll(Builder other)
        {
            return addAll(other.deletedRows);
        }

        private Builder addAll(RoaringBitmap[] deletedRows)
        {
            for (int key = 0; key < deletedRows.length; key++) {
                RoaringBitmap bitmap = deletedRows[key];
                if (bitmap != null && !bitmap.isEmpty()) {
                    getOrCreateBitmap(key).or(bitmap);
                }
            }
            return this;
        }

        private RoaringBitmap getOrCreateBitmap(int key)
        {
            if (key >= deletedRows.length) {
                deletedRows = Arrays.copyOf(deletedRows, key + 1);
            }
            RoaringBitmap bitmap = deletedRows[key];
            if (bitmap == null) {
                bitmap = new RoaringBitmap();
                deletedRows[key] = bitmap;
                bitmapCount++;
            }
            return bitmap;
        }

        public Optional<DeletionVector> build()
        {
            if (bitmapCount == 0) {
                return Optional.empty();
            }
            return Optional.of(new DeletionVector(bitmapCount, deletedRows));
        }
    }
}
