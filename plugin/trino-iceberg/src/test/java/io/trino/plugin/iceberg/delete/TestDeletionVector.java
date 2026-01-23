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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestDeletionVector
{
    @Test
    void testEmptyBuilder()
    {
        assertThat(DeletionVector.builder().build()).isEmpty();
    }

    @Test
    void testBasicOperations()
    {
        DeletionVector vector = DeletionVector.builder()
                .add(0)
                .add(5)
                .add(100)
                .build()
                .orElseThrow();

        assertThat(vector.isRowDeleted(0)).isTrue();
        assertThat(vector.isRowDeleted(5)).isTrue();
        assertThat(vector.isRowDeleted(100)).isTrue();
        assertThat(vector.isRowDeleted(1)).isFalse();
        assertThat(vector.isRowDeleted(99)).isFalse();
        assertThat(vector.cardinality()).isEqualTo(3);
    }

    @Test
    void testLargePositions()
    {
        long largePos = (1L << 33) + 42;
        DeletionVector vector = DeletionVector.builder()
                .add(largePos)
                .add(10)
                .build()
                .orElseThrow();

        assertThat(vector.isRowDeleted(largePos)).isTrue();
        assertThat(vector.isRowDeleted(10)).isTrue();
        assertThat(vector.isRowDeleted(42)).isFalse();
        assertThat(vector.cardinality()).isEqualTo(2);
    }

    @Test
    void testForEachDeletedRow()
    {
        DeletionVector vector = DeletionVector.builder()
                .add(1)
                .add(3)
                .add(5)
                .build()
                .orElseThrow();

        List<Long> deletedRows = new ArrayList<>();
        vector.forEachDeletedRow(deletedRows::add);

        assertThat(deletedRows).containsExactly(1L, 3L, 5L);
    }

    @Test
    void testSerializeDeserialize()
    {
        DeletionVector original = DeletionVector.builder()
                .add(1)
                .add(100)
                .add(1000)
                .build()
                .orElseThrow();

        Slice serialized = original.serialize();
        DeletionVector deserialized = DeletionVector.builder()
                .deserialize(serialized)
                .build()
                .orElseThrow();

        assertThat(deserialized.cardinality()).isEqualTo(original.cardinality());
        assertThat(deserialized.isRowDeleted(1)).isTrue();
        assertThat(deserialized.isRowDeleted(100)).isTrue();
        assertThat(deserialized.isRowDeleted(1000)).isTrue();
        assertThat(deserialized.isRowDeleted(2)).isFalse();
    }

    @Test
    void testSerializeDeserializeMultiplePositionsSameKey()
    {
        // Multiple positions in the same bitmap (same high 32 bits)
        DeletionVector original = DeletionVector.builder()
                .add(1)
                .add(2)
                .add(3)
                .add(100)
                .add(1000)
                .add(10000)
                .build()
                .orElseThrow();

        Slice serialized = original.serialize();
        DeletionVector deserialized = DeletionVector.builder()
                .deserialize(serialized)
                .build()
                .orElseThrow();

        assertThat(deserialized.cardinality()).isEqualTo(6);
        assertThat(deserialized.isRowDeleted(1)).isTrue();
        assertThat(deserialized.isRowDeleted(2)).isTrue();
        assertThat(deserialized.isRowDeleted(3)).isTrue();
        assertThat(deserialized.isRowDeleted(100)).isTrue();
        assertThat(deserialized.isRowDeleted(1000)).isTrue();
        assertThat(deserialized.isRowDeleted(10000)).isTrue();
        assertThat(deserialized.isRowDeleted(4)).isFalse();
    }

    @Test
    void testAddAllDeletionVector()
    {
        DeletionVector first = DeletionVector.builder()
                .add(1)
                .add(2)
                .build()
                .orElseThrow();

        DeletionVector second = DeletionVector.builder()
                .add(3)
                .add(4)
                .build()
                .orElseThrow();

        DeletionVector merged = DeletionVector.builder()
                .addAll(first)
                .addAll(second)
                .build()
                .orElseThrow();

        assertThat(merged.cardinality()).isEqualTo(4);
        assertThat(merged.isRowDeleted(1)).isTrue();
        assertThat(merged.isRowDeleted(2)).isTrue();
        assertThat(merged.isRowDeleted(3)).isTrue();
        assertThat(merged.isRowDeleted(4)).isTrue();
    }

    @Test
    void testAddAllBuilder()
    {
        DeletionVector.Builder first = DeletionVector.builder()
                .add(10)
                .add(20);

        DeletionVector.Builder second = DeletionVector.builder()
                .add(30)
                .add(40);

        DeletionVector merged = first.addAll(second).build().orElseThrow();

        assertThat(merged.cardinality()).isEqualTo(4);
        assertThat(merged.isRowDeleted(10)).isTrue();
        assertThat(merged.isRowDeleted(20)).isTrue();
        assertThat(merged.isRowDeleted(30)).isTrue();
        assertThat(merged.isRowDeleted(40)).isTrue();
    }

    @Test
    void testSparseArrayKeys()
    {
        long key0Pos = 100;
        long key3Pos = (3L << 32) + 200;

        DeletionVector vector = DeletionVector.builder()
                .add(key0Pos)
                .add(key3Pos)
                .build()
                .orElseThrow();

        assertThat(vector.isRowDeleted(key0Pos)).isTrue();
        assertThat(vector.isRowDeleted(key3Pos)).isTrue();
        assertThat(vector.isRowDeleted((1L << 32) + 100)).isFalse();
        assertThat(vector.isRowDeleted((2L << 32) + 100)).isFalse();
        assertThat(vector.cardinality()).isEqualTo(2);

        List<Long> deletedRows = new ArrayList<>();
        vector.forEachDeletedRow(deletedRows::add);
        assertThat(deletedRows).containsExactly(key0Pos, key3Pos);
    }

    @Test
    void testSerializeDeserializeLargePositions()
    {
        long key0Pos = 42;
        long key2Pos = (2L << 32) + 100;
        long key2Pos2 = (2L << 32) + 1000;
        long key2Pos3 = (2L << 32) + 10000;
        long key5Pos = (5L << 32) + 999;
        long key5Pos2 = (5L << 32) + 1000;
        long key5Pos3 = (5L << 32) + 1111;

        DeletionVector original = DeletionVector.builder()
                .add(key0Pos)
                .add(key2Pos)
                .add(key2Pos2)
                .add(key2Pos3)
                .add(key5Pos)
                .add(key5Pos2)
                .add(key5Pos3)
                .build()
                .orElseThrow();

        Slice serialized = original.serialize();
        DeletionVector deserialized = DeletionVector.builder()
                .deserialize(serialized)
                .build()
                .orElseThrow();

        assertThat(deserialized.cardinality()).isEqualTo(7);
        assertThat(deserialized.isRowDeleted(key0Pos)).isTrue();

        assertThat(deserialized.isRowDeleted(key2Pos - 1)).isFalse();
        assertThat(deserialized.isRowDeleted(key2Pos)).isTrue();
        assertThat(deserialized.isRowDeleted(key2Pos2)).isTrue();
        assertThat(deserialized.isRowDeleted(key2Pos3)).isTrue();
        assertThat(deserialized.isRowDeleted(key2Pos3 + 1)).isFalse();

        assertThat(deserialized.isRowDeleted(key5Pos - 1)).isFalse();
        assertThat(deserialized.isRowDeleted(key5Pos)).isTrue();
        assertThat(deserialized.isRowDeleted(key5Pos2)).isTrue();
        assertThat(deserialized.isRowDeleted(key5Pos3)).isTrue();
        assertThat(deserialized.isRowDeleted(key5Pos3 + 1)).isFalse();

        assertThat(deserialized.isRowDeleted((1L << 32) + 100)).isFalse();
    }

    @Test
    void testDeserializeThenAdd()
    {
        DeletionVector original = DeletionVector.builder()
                .add(1)
                .add(2)
                .build()
                .orElseThrow();

        Slice serialized = original.serialize();
        DeletionVector merged = DeletionVector.builder()
                .deserialize(serialized)
                .add(3)
                .add(4)
                .build()
                .orElseThrow();

        assertThat(merged.cardinality()).isEqualTo(4);
        assertThat(merged.isRowDeleted(1)).isTrue();
        assertThat(merged.isRowDeleted(2)).isTrue();
        assertThat(merged.isRowDeleted(3)).isTrue();
        assertThat(merged.isRowDeleted(4)).isTrue();
    }
}
