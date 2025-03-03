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
package org.apache.iceberg.deletes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.delete.DeletionVectors.CRC_SIZE_BYTES;
import static io.trino.plugin.iceberg.delete.DeletionVectors.LENGTH_SIZE_BYTES;

// Exposes package-private BitmapPositionDeleteIndex
public class TrinoBitmapPositionDeleteIndex
        extends BitmapPositionDeleteIndex
{
    private static final int BITMAP_DATA_OFFSET = 4;
    private static final int MAGIC_NUMBER = 1681511377;

    public static TrinoRoaringPositionBitmap deserialize(byte[] bytes, long recordCount, Long contentSizeInBytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int bitmapDataLength = readBitmapDataLength(buffer, contentSizeInBytes);
        TrinoRoaringPositionBitmap bitmap = deserializeBitmap(bytes, bitmapDataLength, recordCount);
        int crc = computeChecksum(bytes, bitmapDataLength);
        int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
        int expectedCrc = buffer.getInt(crcOffset);
        checkArgument(crc == expectedCrc, "Invalid CRC");
        return bitmap;
    }

    private static int readBitmapDataLength(ByteBuffer buffer, Long contentSizeInBytes)
    {
        int length = buffer.getInt();
        long expectedLength = contentSizeInBytes - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
        checkArgument(length == expectedLength, "Invalid bitmap data length: %s, expected %s", length, expectedLength);
        return length;
    }

    private static TrinoRoaringPositionBitmap deserializeBitmap(byte[] bytes, int bitmapDataLength, long recordCount)
    {
        ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
        int magicNumber = bitmapData.getInt();
        checkArgument(magicNumber == MAGIC_NUMBER, "Invalid magic number: %s, expected %s", magicNumber, MAGIC_NUMBER);
        TrinoRoaringPositionBitmap bitmap = TrinoRoaringPositionBitmap.deserialize(bitmapData);
        long cardinality = bitmap.cardinality();
        checkArgument(cardinality == recordCount, "Invalid cardinality: %s, expected %s", cardinality, recordCount);
        return bitmap;
    }

    private static int computeChecksum(byte[] bytes, int bitmapDataLength)
    {
        CRC32 crc = new CRC32();
        crc.update(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        return (int) crc.getValue();
    }

    private static ByteBuffer pointToBitmapData(byte[] bytes, int bitmapDataLength)
    {
        ByteBuffer bitmapData = ByteBuffer.wrap(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        bitmapData.order(ByteOrder.LITTLE_ENDIAN);
        return bitmapData;
    }
}
