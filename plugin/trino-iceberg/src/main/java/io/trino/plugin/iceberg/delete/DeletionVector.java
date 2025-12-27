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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public final class DeletionVector
{
    private static final int MAGIC_LE = 0x64_39_d3_d1;
    private static final int LENGTH_SIZE_BYTES = 4;
    private static final int CRC_SIZE_BYTES = 4;

    private final RoaringBitmap[] deletedRows;

    private DeletionVector(RoaringBitmap[] deletedRows)
    {
        this.deletedRows = deletedRows;
    }

    public boolean isRowDeleted(long filePos)
    {
        RoaringBitmap deletedRow = deletedRows[(int) (filePos >>> 32)];
        return deletedRow != null && deletedRow.contains((int) filePos);
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
        return keyLow -> consumer.accept(((long) keyHigh << 32) | (keyLow & 0xFFFFFFFFL));
    }

    public Slice serialize()
    {
        // NOTE: the deletion vector randomly mixes big-endian and little-endian integers, so be very careful in this code

        // calculate bodyLength
        int bodyLength = Integer.BYTES; // magic number
        bodyLength += SIZE_OF_LONG; // bitmap count
        int bitmapCount = 0;
        for (RoaringBitmap bitmap : deletedRows) {
            if (bitmap != null && !bitmap.isEmpty()) {
                bodyLength += Integer.BYTES; // key
                bodyLength += bitmap.serializedSizeInBytes(); // bitmap
                bitmapCount++;
            }
        }

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
        int actualCrc = (int) crc.getValue();
        return actualCrc;
    }

    public long cardinality()
    {
        return Arrays.stream(deletedRows)
                .filter(Objects::nonNull)
                .mapToLong(RoaringBitmap::getLongCardinality)
                .sum();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        // key = (int) (pos >>> 32), value bitmap contains (int) pos low bits
        private final Int2ObjectMap<RoaringBitmap> deletedRows = new Int2ObjectOpenHashMap<>();

        public Builder deserialize(Slice data)
        {
            // NOTE: the deletion vector randomly mixes big-endian and little-endian integers, so be very careful in this code

            int offset = 0;

            // read and validate length
            int length = Integer.reverseBytes(data.getInt(offset));
            offset += Integer.BYTES;
            long expectedLength = data.length() - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
            checkArgument(length == expectedLength, "Invalid bitmap data length: %s, expected %s", length, expectedLength);

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
                // key is the high 312 bits of the file position
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
                    throw new RuntimeException("Failed to deserialize deletion vector bitmap for key: " + key, e);
                }
                offset += bitmap.serializedSizeInBytes();

                deletedRows.merge(key, bitmap, (existing, newBitmap) -> {
                    existing.or(newBitmap);
                    return existing;
                });

                lastKey = key;
            }

            // verify checksum
            checkArgument(crc32(data, LENGTH_SIZE_BYTES, length) == Integer.reverseBytes(data.getInt(offset)), "Invalid CRC");
            return this;
        }

        public Builder add(long pos)
        {
            int key = (int) (pos >>> 32);
            int low = (int) pos;
            deletedRows.computeIfAbsent(key, _ -> new RoaringBitmap()).add(low);
            return this;
        }

        public Builder addAll(DeletionVector deletionVector)
        {
            for (int i = 0; i < deletionVector.deletedRows.length; i++) {
                RoaringBitmap deletedRow = deletionVector.deletedRows[i];
                if (deletedRow != null && !deletedRow.isEmpty()) {
                    deletedRows.computeIfAbsent(i, _ -> new RoaringBitmap()).or(deletedRow);
                }
            }
            return this;
        }

        public Builder addAll(Builder other)
        {
            other.deletedRows.forEach((key, bitmap) -> deletedRows.computeIfAbsent(key, _ -> new RoaringBitmap()).or(bitmap));
            return this;
        }

        public Optional<DeletionVector> build()
        {
            if (deletedRows.isEmpty()) {
                return Optional.empty();
            }
            int maxKey = deletedRows.keySet().intStream().max().orElse(0);
            RoaringBitmap[] deletedRows = new RoaringBitmap[maxKey + 1];
            this.deletedRows.forEach((key, bitmap) -> deletedRows[key] = bitmap);
            return Optional.of(new DeletionVector(deletedRows));
        }
    }
}
