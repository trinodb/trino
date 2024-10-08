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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestRoaringBitmapArray
{
    @Test
    void testIsEmpty()
    {
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        assertThat(bitmaps.isEmpty()).isTrue();

        bitmaps.add(0);
        assertThat(bitmaps.isEmpty()).isFalse();
    }

    @Test
    void testLength()
    {
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        assertThat(bitmaps.length()).isZero();

        bitmaps.add(0);
        assertThat(bitmaps.length()).isEqualTo(1);

        bitmaps.add(1);
        assertThat(bitmaps.length()).isEqualTo(1);
    }

    @Test
    void testCardinality()
    {
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        assertThat(bitmaps.cardinality()).isZero();

        bitmaps.add(0);
        assertThat(bitmaps.cardinality()).isEqualTo(1);

        bitmaps.add(1);
        assertThat(bitmaps.cardinality()).isEqualTo(2);
    }

    @Test
    void testSerializedSizeInBytes()
    {
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        assertThat(bitmaps.serializedSizeInBytes()).isZero();

        bitmaps.add(0);
        assertThat(bitmaps.serializedSizeInBytes()).isEqualTo(22);

        bitmaps.add(1);
        assertThat(bitmaps.serializedSizeInBytes()).isEqualTo(24);
    }

    @Test
    void testAdd()
    {
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        bitmaps.add(0);

        assertThat(bitmaps.contains(0)).isTrue();
        assertThat(bitmaps.contains(1)).isFalse();

        bitmaps.add((long) Integer.MAX_VALUE + 1);
        assertThat(bitmaps.contains(Integer.MAX_VALUE)).isFalse();
        assertThat(bitmaps.contains((long) Integer.MAX_VALUE + 1)).isTrue();
        assertThat(bitmaps.contains((long) Integer.MAX_VALUE + 2)).isFalse();
    }

    @Test
    void testAddRange()
    {
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        bitmaps.addRange(3, 5);

        assertThat(bitmaps.contains(2)).isFalse();
        assertThat(bitmaps.contains(3)).isTrue();
        assertThat(bitmaps.contains(4)).isTrue();
        assertThat(bitmaps.contains(5)).isTrue();
        assertThat(bitmaps.contains(6)).isFalse();

        bitmaps.addRange(7, 8);

        assertThat(bitmaps.contains(2)).isFalse();
        assertThat(bitmaps.contains(3)).isTrue();
        assertThat(bitmaps.contains(4)).isTrue();
        assertThat(bitmaps.contains(5)).isTrue();
        assertThat(bitmaps.contains(6)).isFalse();
        assertThat(bitmaps.contains(7)).isTrue();
        assertThat(bitmaps.contains(8)).isTrue();
        assertThat(bitmaps.contains(9)).isFalse();
    }

    @Test
    void testOr()
    {
        RoaringBitmapArray bitmapsOr = new RoaringBitmapArray();
        bitmapsOr.add(2);
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        bitmaps.add(3);
        bitmapsOr.or(bitmaps);

        assertThat(bitmapsOr.contains(1)).isFalse();
        assertThat(bitmapsOr.contains(2)).isTrue();
        assertThat(bitmapsOr.contains(3)).isTrue();
        assertThat(bitmapsOr.contains(4)).isFalse();
    }

    @Test
    void testAndNot()
    {
        RoaringBitmapArray bitmapsAndNot = new RoaringBitmapArray();
        bitmapsAndNot.addRange(1, 5);
        RoaringBitmapArray bitmaps = new RoaringBitmapArray();
        bitmaps.addRange(2, 4);
        bitmapsAndNot.andNot(bitmaps);

        assertThat(bitmapsAndNot.contains(1)).isTrue();
        assertThat(bitmapsAndNot.contains(2)).isFalse();
        assertThat(bitmapsAndNot.contains(3)).isFalse();
        assertThat(bitmapsAndNot.contains(4)).isFalse();
        assertThat(bitmapsAndNot.contains(5)).isTrue();
    }
}
