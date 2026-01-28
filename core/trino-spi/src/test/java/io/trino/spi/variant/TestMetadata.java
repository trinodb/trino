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
package io.trino.spi.variant;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.variant.Header.metadataIsSorted;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA_SLICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMetadata
{
    // from the Iceberg implementation, an empty metadata has 3 bytes: header + offsetCount + 0th offset
    private static final Slice SERIALIZED_EMPTY_METADATA = wrappedBuffer(new byte[] {0x01, 0x00, 0x00});

    @Test
    void testEmpty()
    {
        assertThat(EMPTY_METADATA_SLICE).isEqualTo(SERIALIZED_EMPTY_METADATA);

        assertThat(Metadata.of(List.of())).isSameAs(EMPTY_METADATA);
        assertThat(EMPTY_METADATA.dictionarySize()).isEqualTo(0);
        assertThat(EMPTY_METADATA.toSlice()).isSameAs(EMPTY_METADATA_SLICE);
        assertThat(EMPTY_METADATA.id(Slices.utf8Slice("non_existent_field"))).isEqualTo(-1);
        assertThatThrownBy(() -> EMPTY_METADATA.get(0)).isInstanceOf(IndexOutOfBoundsException.class);

        EMPTY_METADATA.validateFully();

        // double check compatibility with Iceberg serialization
        assertThat(serializeIcebergVariantMetadata(Variants.emptyMetadata())).isEqualTo(SERIALIZED_EMPTY_METADATA);
    }

    @Test
    void testSortedFields()
    {
        // one byte offsets
        assertMetadata(List.of(Slices.utf8Slice("apple"), Slices.utf8Slice("banana"), Slices.utf8Slice("cherry")), true);
        // two bytes offsets (total size between 256 and 65,535 bytes)
        assertMetadata(generateFieldNames(100), true);
        // two bytes offsets (total size between 65,536 and 16,777,215 bytes)
        assertMetadata(generateFieldNames(10_000), true);
        // three bytes offsets (total size over 16,777,215 bytes)
        assertMetadata(generateFieldNames(5_000_000), true);
    }

    private static List<Slice> generateFieldNames(int endExclusive)
    {
        return IntStream.range(0, endExclusive).mapToObj(i -> Slices.utf8Slice("field_%7d".formatted(i))).toList();
    }

    @Test
    void testUnorderedFields()
    {
        assertMetadata(List.of(Slices.utf8Slice("banana"), Slices.utf8Slice("cherry"), Slices.utf8Slice("apple")), false);
        List<Slice> fields = Arrays.asList(IntStream.range(0, 1000).mapToObj(i -> Slices.utf8Slice("field_" + i)).toArray(Slice[]::new));
        Collections.shuffle(fields);
        assertMetadata(fields, false);
    }

    private static void assertMetadata(List<Slice> fieldNames, boolean expectedSorted)
    {
        Metadata metadata = Metadata.of(fieldNames);
        metadata.validateFully();
        assertThat(metadataIsSorted(metadata.toSlice().getByte(0))).isEqualTo(expectedSorted);
        assertThat(metadata.dictionarySize()).isEqualTo(fieldNames.size());

        // verify lookups by index
        for (int i = 0; i < metadata.dictionarySize(); i++) {
            assertThat(metadata.get(i)).isEqualTo(fieldNames.get(i));
        }

        // verify lookups by name
        if (fieldNames.size() <= 500) {
            for (int i = 0; i < fieldNames.size(); i++) {
                assertThat(metadata.id(fieldNames.get(i))).isEqualTo(i);
            }
        }
        else {
            // for large dictionaries, select random lookups to verify
            ThreadLocalRandom.current().ints(500, 0, fieldNames.size())
                    .forEach(i -> assertThat(metadata.id(fieldNames.get(i))).isEqualTo(i));
        }
        assertThat(metadata.id(Slices.utf8Slice(fieldNames.getFirst().toStringUtf8() + " non_existent_field"))).isEqualTo(-1);

        // verify direct construction creates same metadata
        Metadata actual = Metadata.of(fieldNames);
        assertThat(actual).isEqualTo(metadata);

        // verify serialization
        assertThat(metadata.toSlice()).isEqualTo(serializeIcebergVariantMetadata(fieldNames));
    }

    private static Slice serializeIcebergVariantMetadata(List<Slice> fieldNames)
    {
        var metadata = Variants.metadata(fieldNames.stream().map(Slice::toStringUtf8).toList());
        return serializeIcebergVariantMetadata(metadata);
    }

    private static Slice serializeIcebergVariantMetadata(org.apache.iceberg.variants.VariantMetadata metadata)
    {
        int size = metadata.sizeInBytes();
        byte[] array = new byte[size];
        ByteBuffer valueBuf = ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN);
        metadata.writeTo(valueBuf, 0);
        return wrappedBuffer(array);
    }
}
