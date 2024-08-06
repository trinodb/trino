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

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;

/**
 * A simplified version of <a href="https://github.com/delta-io/delta/blob/455dbac/spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArray.scala">RoaringBitmapArray</a>
 */
public final class RoaringBitmapArray
{
    // Must bitmask to avoid sign extension
    private static final long MAX_REPRESENTABLE_VALUE = (((long) Integer.MAX_VALUE - 1) << 32) | (((long) Integer.MIN_VALUE) & 0xFFFFFFFFL);
    private static final int INDIVIDUAL_BITMAP_KEY_SIZE = 4;

    private RoaringBitmap[] bitmaps = new RoaringBitmap[0];

    public RoaringBitmapArray(Roaring64Bitmap bitmap)
    {
        bitmap.forEach(this::add);
    }

    private void add(long value)
    {
        checkArgument(value >= 0 && value <= MAX_REPRESENTABLE_VALUE, "Unsupported value: %s", value);

        int high = (int) (value >> 32);
        int low = (int) value;

        if (high >= bitmaps.length) {
            extendBitmaps(high + 1);
        }
        RoaringBitmap highBitmap = bitmaps[high];
        highBitmap.add(low);
    }

    private void extendBitmaps(int newLength)
    {
        if (bitmaps.length == 0 && newLength == 1) {
            bitmaps = new RoaringBitmap[] {new RoaringBitmap()};
            return;
        }
        RoaringBitmap[] newBitmaps = new RoaringBitmap[newLength];
        System.arraycopy(bitmaps, 0, newBitmaps, 0, bitmaps.length);
        for (int i = bitmaps.length; i < newLength; i++) {
            newBitmaps[i] = new RoaringBitmap();
        }
        bitmaps = newBitmaps;
    }

    public RoaringBitmap[] bitmaps()
    {
        return bitmaps.clone();
    }

    public int serializedSizeInBytes()
    {
        long size = 0;
        for (RoaringBitmap bitmap : bitmaps) {
            size += bitmap.serializedSizeInBytes() + INDIVIDUAL_BITMAP_KEY_SIZE;
        }
        return toIntExact(size);
    }
}
