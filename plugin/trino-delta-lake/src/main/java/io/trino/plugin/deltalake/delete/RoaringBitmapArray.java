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

import com.google.common.primitives.UnsignedInts;
import org.roaringbitmap.RoaringBitmap;

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

    public RoaringBitmap get(int i)
    {
        return bitmaps[i];
    }

    public boolean contains(long value)
    {
        checkArgument(value >= 0 && value <= MAX_REPRESENTABLE_VALUE, "Unsupported value: %s", value);

        int high = highBytes(value);
        if (high >= bitmaps.length) {
            return false;
        }
        int low = lowBytes(value);
        return bitmaps[high].contains(low);
    }

    public boolean isEmpty()
    {
        for (RoaringBitmap bitmap : bitmaps) {
            if (!bitmap.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public long length()
    {
        return bitmaps.length;
    }

    public long cardinality()
    {
        long sum = 0;
        for (RoaringBitmap bitmap : bitmaps) {
            sum += bitmap.getLongCardinality();
        }
        return sum;
    }

    public int serializedSizeInBytes()
    {
        long size = 0;
        for (RoaringBitmap bitmap : bitmaps) {
            size += bitmap.serializedSizeInBytes() + INDIVIDUAL_BITMAP_KEY_SIZE;
        }
        return toIntExact(size);
    }

    public void add(long value)
    {
        checkArgument(value >= 0 && value <= MAX_REPRESENTABLE_VALUE, "Unsupported value: %s", value);

        int high = highBytes(value);
        int low = lowBytes(value);

        if (high >= bitmaps.length) {
            extendBitmaps(high + 1);
        }
        RoaringBitmap highBitmap = bitmaps[high];
        highBitmap.add(low);
    }

    /**
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd exclusive ending of range
     */
    public void addRange(long rangeStart, long rangeEnd)
    {
        checkArgument(rangeStart >= 0 && rangeStart <= rangeEnd, "Unsupported value: %s", rangeStart);
        if (bitmaps.length == 0) {
            extendBitmaps(1);
        }
        int startHigh = highBytes(rangeStart);
        int startLow = lowBytes(rangeStart);

        int endHigh = highBytes(rangeEnd);
        int endLow = lowBytes(rangeEnd);

        int lastHigh = endHigh;

        if (lastHigh >= bitmaps.length) {
            extendBitmaps(lastHigh + 1);
        }

        int currentHigh = startHigh;
        while (currentHigh <= lastHigh) {
            long start = currentHigh == startHigh ? UnsignedInts.toLong(startLow) : 0L;
            // RoaringBitmap.add is exclusive the end boundary.
            long end = currentHigh == endHigh ? UnsignedInts.toLong(endLow) + 1L : 0xFFFFFFFFL + 1L;
            bitmaps[currentHigh].add(start, end);
            currentHigh += 1;
        }
    }

    public void or(RoaringBitmapArray other)
    {
        if (bitmaps.length < other.bitmaps.length) {
            extendBitmaps(other.bitmaps.length);
        }
        for (int i = 0; i < other.bitmaps.length; i++) {
            bitmaps[i].or(other.bitmaps[i]);
        }
    }

    public void andNot(RoaringBitmapArray other)
    {
        int length = Math.min(bitmaps.length, other.bitmaps.length);
        for (int i = 0; i < length; i++) {
            bitmaps[i].andNot(other.bitmaps[i]);
        }
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

    private static int highBytes(long value)
    {
        return (int) (value >> 32);
    }

    private static int lowBytes(long value)
    {
        return (int) value;
    }
}
