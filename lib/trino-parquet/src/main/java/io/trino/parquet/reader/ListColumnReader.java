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

import io.trino.parquet.Field;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.isOptionalFieldValueNull;

public final class ListColumnReader
{
    private ListColumnReader() {}

    /**
     * Each collection (Array or Map) has four variants of presence:
     * 1) Collection is not defined, because one of it's optional parent fields is null
     * 2) Collection is null
     * 3) Collection is defined but empty
     * 4) Collection is defined and not empty. In this case offset value is increased by the number of elements in that collection
     */
    public static BlockPositions calculateCollectionOffsets(Field field, int[] definitionLevels, int[] repetitionLevels)
    {
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxElementRepetitionLevel = field.getRepetitionLevel() + 1;
        boolean required = field.isRequired();
        int offset = 0;
        IntArrayList offsets = new IntArrayList();
        offsets.add(offset);
        if (required) {
            for (int i = 0; i < definitionLevels.length; i = getNextCollectionStartIndex(repetitionLevels, maxElementRepetitionLevel, i)) {
                if (definitionLevels[i] == maxDefinitionLevel) {
                    // Collection is defined but empty
                    offsets.add(offset);
                }
                else if (definitionLevels[i] > maxDefinitionLevel) {
                    // Collection is defined and not empty
                    offset += getCollectionSize(repetitionLevels, maxElementRepetitionLevel, i + 1);
                    offsets.add(offset);
                }
            }
            return new BlockPositions(Optional.empty(), offsets.toIntArray());
        }

        BooleanArrayList collectionIsNull = new BooleanArrayList();
        int nullValuesCount = 0;
        for (int i = 0; i < definitionLevels.length; i = getNextCollectionStartIndex(repetitionLevels, maxElementRepetitionLevel, i)) {
            if (definitionLevels[i] >= maxDefinitionLevel - 1) {
                boolean isNull = isOptionalFieldValueNull(definitionLevels[i], maxDefinitionLevel);
                collectionIsNull.add(isNull);
                nullValuesCount += isNull ? 1 : 0;
                // definitionLevels[i] == maxDefinitionLevel - 1 => Collection is null
                // definitionLevels[i] == maxDefinitionLevel     => Collection is defined but empty
                if (definitionLevels[i] > maxDefinitionLevel) {
                    // Collection is defined and not empty
                    offset += getCollectionSize(repetitionLevels, maxElementRepetitionLevel, i + 1);
                }
                offsets.add(offset);
            }
        }
        if (nullValuesCount == 0) {
            return new BlockPositions(Optional.empty(), offsets.toIntArray());
        }
        return new BlockPositions(Optional.of(collectionIsNull.elements()), offsets.toIntArray());
    }

    public record BlockPositions(Optional<boolean[]> isNull, int[] offsets) {}

    private static int getNextCollectionStartIndex(int[] repetitionLevels, int maxRepetitionLevel, int elementIndex)
    {
        do {
            elementIndex++;
        }
        while (hasMoreElements(repetitionLevels, elementIndex) && !isCollectionBeginningMarker(repetitionLevels, maxRepetitionLevel, elementIndex));
        return elementIndex;
    }

    /**
     * This method is only called for non-empty collections
     */
    private static int getCollectionSize(int[] repetitionLevels, int maxRepetitionLevel, int nextIndex)
    {
        int size = 1;
        while (hasMoreElements(repetitionLevels, nextIndex) && !isCollectionBeginningMarker(repetitionLevels, maxRepetitionLevel, nextIndex)) {
            // Collection elements cannot only be primitive, but also can have nested structure
            // Counting only elements which belong to current collection, skipping inner elements of nested collections/structs
            if (repetitionLevels[nextIndex] <= maxRepetitionLevel) {
                size++;
            }
            nextIndex++;
        }
        return size;
    }

    private static boolean isCollectionBeginningMarker(int[] repetitionLevels, int maxRepetitionLevel, int nextIndex)
    {
        return repetitionLevels[nextIndex] < maxRepetitionLevel;
    }

    private static boolean hasMoreElements(int[] repetitionLevels, int nextIndex)
    {
        return nextIndex < repetitionLevels.length;
    }
}
