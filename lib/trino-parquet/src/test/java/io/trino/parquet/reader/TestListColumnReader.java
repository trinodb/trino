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
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3, 3, 3}, new int[] {0, 1, 1, 0, 1, 1}, new int[] {0, 3, 6}, false);
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 2, 2}, new int[] {0, 1, 0, 1}, new int[] {0, 2, 4}, false);
        // A single element per collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3}, new int[] {0, 0, 0}, new int[] {0, 1, 2, 3}, false);
        // Elements of the collection nested inside have repetition level 2
        assertOffsets(INNER_LIST, new int[] {5, 5, 5, 5, 5, 5}, new int[] {0, 2, 2, 0, 2, 2}, new int[] {0, 3, 6}, false);
        // Null elements do not change offsets
        assertOffsets(OPTIONAL_LIST, new int[] {3, 2, 3, 2, 3, 3}, new int[] {0, 1, 1, 0, 1, 1}, new int[] {0, 3, 6}, false);
        // The only collection of the batch
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 2, 2}, new int[] {0, 1, 1, 1}, new int[] {0, 4}, false);
    }

    @Test
    public void testNonFixedSizeCollections()
    {
        // Variable sizes summing to a multiple of the first size
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3, 3, 3, 3, 3}, new int[] {0, 1, 0, 1, 1, 1, 1, 1}, new int[] {0, 2, 8}, false);
        // A null collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 0, 3, 3}, new int[] {0, 1, 0, 0, 1}, new int[] {0, 2, 2, 4}, true);
        // An empty collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 1, 3, 3}, new int[] {0, 1, 0, 0, 1}, new int[] {0, 2, 2, 4}, false);
        assertOffsets(REQUIRED_LIST, new int[] {2, 2, 0, 2, 2}, new int[] {0, 1, 0, 0, 1}, new int[] {0, 2, 2, 4}, false);
        // Elements nested in the elements of a collection have a higher repetition level and are not counted
        assertOffsets(INNER_LIST, new int[] {5, 5, 5, 5, 5, 5}, new int[] {0, 3, 2, 0, 3, 2}, new int[] {0, 2, 4}, false);
        // The batch starts in the middle of a collection
        assertOffsets(OPTIONAL_LIST, new int[] {3, 3, 3, 3}, new int[] {1, 1, 0, 1}, new int[] {0, 2, 4}, false);
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

    private static void assertOffsets(GroupField field, int[] definitionLevels, int[] repetitionLevels, int[] expectedOffsets, boolean expectNulls)
    {
        BlockPositions actual = calculateCollectionOffsets(field, definitionLevels, repetitionLevels);
        assertThat(actual.offsets()).isEqualTo(expectedOffsets);
        assertThat(actual.valueIsValid().isPresent()).isEqualTo(expectNulls);
        BlockPositions general = calculateAnyCollectionOffsets(field, definitionLevels, repetitionLevels);
        assertThat(actual.offsets()).isEqualTo(general.offsets());
    }

    private static GroupField listField(int repetitionLevel, int definitionLevel, boolean required)
    {
        return new GroupField(new ArrayType(INTEGER), repetitionLevel, definitionLevel, required, List.of(Optional.empty()));
    }
}
