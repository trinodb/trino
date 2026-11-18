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
package io.trino.parquet.reader;

import io.trino.parquet.GroupField;
import io.trino.parquet.reader.ListColumnReader.BlockPositions;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static io.trino.parquet.reader.ListColumnReader.calculateAnyCollectionOffsets;
import static io.trino.parquet.reader.ListColumnReader.calculateCollectionOffsets;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestListColumnReader
{
    // optional list<optional int>: null list 0, empty list 1, null element 2, element 3
    private static final GroupField OPTIONAL_LIST = listField(0, 1, false);
    // required list<optional int>: empty list 0, null element 1, element 2
    private static final GroupField REQUIRED_LIST = listField(0, 0, true);
    // optional list<optional list<optional int>>, the inner list
    private static final GroupField INNER_LIST = listField(1, 3, false);

    @Test
    public void testFixedSizeCollections()
    {
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3, 3, 3}, new int[] {0, 1, 1, 0, 1, 1}, new int[] {0, 3, 6});
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 2, 2}, new int[] {0, 1, 0, 1}, new int[] {0, 2, 4});
        // A single element per collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3}, new int[] {0, 0, 0}, new int[] {0, 1, 2, 3});
        // Elements of the collection nested inside have repetition level 2
        assertOffsets(INNER_LIST, new int[] {5, 5, 5, 5, 5, 5}, new int[] {0, 2, 2, 0, 2, 2}, new int[] {0, 3, 6});
        // Null elements do not change offsets
        assertOffsets(OPTIONAL_LIST, new int[] {3, 2, 3, 2, 3, 3}, new int[] {0, 1, 1, 0, 1, 1}, new int[] {0, 3, 6});
        // The only collection of the batch
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 2, 2}, new int[] {0, 1, 1, 1}, new int[] {0, 4});
    }

    @Test
    public void testNonFixedSizeCollections()
    {
        // Variable sizes summing to a multiple of the first size
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3, 3, 3, 3, 3}, new int[] {0, 1, 0, 1, 1, 1, 1, 1}, new int[] {0, 2, 8});
        // A null collection, after a collection of two elements
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 0, 3, 3}, new int[] {0, 1, 0, 0, 1}, new int[] {0, 2, 2, 4}, 1);
        // An empty collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 1, 3, 3}, new int[] {0, 1, 0, 0, 1}, new int[] {0, 2, 2, 4});
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 0, 2, 2}, new int[] {0, 1, 0, 0, 1}, new int[] {0, 2, 2, 4});
        // Elements nested in the elements of a collection have a higher repetition level and are not counted
        assertOffsets(INNER_LIST, new int[] {5, 5, 5, 5, 5, 5}, new int[] {0, 3, 2, 0, 3, 2}, new int[] {0, 2, 4});
        // The batch starts in the middle of a collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3}, new int[] {1, 1, 0, 1}, new int[] {0, 2, 4});
        // A null or empty collection where the next collection of the first size would start
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 0, 3, 3, 3}, new int[] {0, 1, 0, 0, 1, 1}, new int[] {0, 2, 2, 5}, 1);
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 1, 3, 3, 3}, new int[] {0, 1, 0, 0, 1, 1}, new int[] {0, 2, 2, 5});
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 0, 2, 2, 2}, new int[] {0, 1, 0, 0, 1, 1}, new int[] {0, 2, 2, 5});
        // Two collections where one collection of the first size would be
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3, 3, 3}, new int[] {0, 1, 1, 0, 0, 1}, new int[] {0, 3, 4, 6});
        // A collection longer than the first size, so the collection before it is not verified
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3, 3, 3, 3, 3}, new int[] {0, 1, 0, 1, 0, 1, 1, 0}, new int[] {0, 2, 4, 7, 8});
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 2, 2, 2, 2, 2, 2}, new int[] {0, 1, 0, 1, 0, 1, 1, 0}, new int[] {0, 2, 4, 7, 8});
    }

    @Test
    public void testNullCollectionPositions()
    {
        // Collections of two elements with null collections at the given positions. The first null is past the 64th bit of the validity bitmap,
        // so the earlier collections are marked valid in more than one word, and the later nulls are not set again.
        int collections = 140;
        int[] nullCollections = {70, 71, 130};
        int[] definitionLevels = new int[collections * 2 - nullCollections.length];
        int[] repetitionLevels = new int[definitionLevels.length];
        int[] offsets = new int[collections + 1];
        int index = 0;
        for (int collection = 0; collection < collections; collection++) {
            if (Arrays.binarySearch(nullCollections, collection) >= 0) {
                definitionLevels[index] = 0;
                repetitionLevels[index++] = 0;
                offsets[collection + 1] = offsets[collection];
                continue;
            }
            definitionLevels[index] = 3;
            definitionLevels[index + 1] = 3;
            repetitionLevels[index] = 0;
            repetitionLevels[index + 1] = 1;
            index += 2;
            offsets[collection + 1] = offsets[collection] + 2;
        }
        assertOffsets(OPTIONAL_LIST, definitionLevels, repetitionLevels, offsets, nullCollections);
    }

    @Test
    public void testFirstNullAfterValidityWordBoundary()
    {
        int[] definitionLevels = new int[68];
        Arrays.fill(definitionLevels, 3);
        definitionLevels[65] = 0;
        definitionLevels[66] = 1;
        BlockPositions positions = calculateCollectionOffsets(OPTIONAL_LIST, definitionLevels, new int[68]);
        assertThat(positions.valueIsValid().orElseThrow()).containsExactly(-1L, 0b1101L);
        assertThat(Arrays.copyOfRange(positions.offsets(), 65, 69)).containsExactly(65, 65, 65, 66);
    }

    @Test
    public void testRandomizedAgainstGeneralAlgorithm()
    {
        Random random = new Random(1234);
        for (int iteration = 0; iteration < 20_000; iteration++) {
            GroupField field = switch (random.nextInt(3)) {
                case 0 -> OPTIONAL_LIST;
                case 1 -> REQUIRED_LIST;
                default -> INNER_LIST;
            };
            int maxRepetitionLevel = field.getRepetitionLevel() + 1;
            int size = 1 + random.nextInt(6);
            int collections = 1 + random.nextInt(8);
            int[] definition = new int[collections * (size + 1) + 2];
            int[] repetition = new int[definition.length];
            int count = 0;
            for (int collection = 0; collection < collections; collection++) {
                // Mostly fixed size collections with rare anomalies, so that the fast path is taken often
                int collectionSize = size;
                int anomaly = random.nextInt(20);
                if (anomaly == 0) {
                    collectionSize = random.nextInt(size + 2);
                }
                int maxDefinitionLevel = field.getDefinitionLevel();
                if (collectionSize == 0) {
                    definition[count] = maxDefinitionLevel - random.nextInt(2);
                    repetition[count++] = 0;
                    continue;
                }
                for (int element = 0; element < collectionSize; element++) {
                    definition[count] = maxDefinitionLevel + 1 + random.nextInt(2);
                    repetition[count++] = element == 0 ? random.nextInt(maxRepetitionLevel) : maxRepetitionLevel + (random.nextInt(25) == 0 ? 1 : 0);
                }
            }
            int[] definitionLevels = Arrays.copyOf(definition, count);
            int[] repetitionLevels = Arrays.copyOf(repetition, count);

            BlockPositions expected = calculateAnyCollectionOffsets(field, definitionLevels, repetitionLevels);
            BlockPositions actual = calculateCollectionOffsets(field, definitionLevels, repetitionLevels);
            assertThat(actual.offsets()).isEqualTo(expected.offsets());
            assertThat(actual.valueIsValid().isPresent()).isEqualTo(expected.valueIsValid().isPresent());
            actual.valueIsValid().ifPresent(valid -> assertThat(valid).isEqualTo(expected.valueIsValid().orElseThrow()));
        }
    }

    private static void assertOffsets(GroupField field, int[] definitionLevels, int[] repetitionLevels, int[] expectedOffsets, int... expectedNullCollections)
    {
        BlockPositions actual = calculateCollectionOffsets(field, definitionLevels, repetitionLevels);
        assertThat(actual.offsets()).isEqualTo(expectedOffsets);
        assertThat(nullCollections(actual)).containsExactly(expectedNullCollections);
        BlockPositions general = calculateAnyCollectionOffsets(field, definitionLevels, repetitionLevels);
        assertThat(actual.offsets()).isEqualTo(general.offsets());
        assertThat(nullCollections(general)).containsExactly(expectedNullCollections);
    }

    private static int[] nullCollections(BlockPositions positions)
    {
        if (positions.valueIsValid().isEmpty()) {
            return new int[0];
        }
        long[] valid = positions.valueIsValid().get();
        return IntStream.range(0, positions.offsets().length - 1)
                .filter(collection -> (valid[collection >> 6] & (1L << collection)) == 0)
                .toArray();
    }

    private static GroupField listField(int repetitionLevel, int definitionLevel, boolean required)
    {
        return new GroupField(new ArrayType(INTEGER), repetitionLevel, definitionLevel, required, List.of(Optional.empty()));
    }
}
