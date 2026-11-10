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

import com.google.common.annotations.VisibleForTesting;
import io.trino.parquet.Field;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.isOptionalFieldValueNull;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.Bitmap.wordsForBits;

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
        int length = definitionLevels.length;
        // Fast path for batches in which every collection is non-null, non-empty and has the same size,
        // as in vector or embedding columns. The offsets are then a multiple of the collection size,
        // which is taken from the first collection.
        int size = getFixedCollectionSize(maxElementRepetitionLevel, repetitionLevels);
        if (size == 0) {
            return calculateAnyCollectionOffsets(field, definitionLevels, repetitionLevels, 0, 0);
        }
        int collectionCount = length / size;
        int fixedSizeCollections = countFixedSizeCollections(maxDefinitionLevel, maxElementRepetitionLevel, definitionLevels, repetitionLevels, size, collectionCount);
        if (fixedSizeCollections < collectionCount) {
            // The collections verified so far are reused, so a batch that does not have the fixed size shape is not scanned twice
            return calculateAnyCollectionOffsets(field, definitionLevels, repetitionLevels, fixedSizeCollections, size);
        }
        int[] offsets = new int[collectionCount + 1];
        for (int collection = 1; collection <= collectionCount; collection++) {
            offsets[collection] = collection * size;
        }
        return new BlockPositions(Optional.empty(), offsets);
    }

    @VisibleForTesting
    static BlockPositions calculateAnyCollectionOffsets(Field field, int[] definitionLevels, int[] repetitionLevels)
    {
        return calculateAnyCollectionOffsets(field, definitionLevels, repetitionLevels, 0, 0);
    }

    /**
     * The first {@code fixedSizeCollections} collections are known to be non-null and to have {@code fixedSize} elements each
     */
    private static BlockPositions calculateAnyCollectionOffsets(Field field, int[] definitionLevels, int[] repetitionLevels, int fixedSizeCollections, int fixedSize)
    {
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxElementRepetitionLevel = field.getRepetitionLevel() + 1;
        boolean required = field.isRequired();
        int offset = 0;
        IntArrayList offsets = new IntArrayList();
        offsets.add(offset);
        for (int collection = 0; collection < fixedSizeCollections; collection++) {
            offset += fixedSize;
            offsets.add(offset);
        }
        int firstIndex = fixedSizeCollections * fixedSize;
        if (required) {
            for (int i = firstIndex; i < definitionLevels.length; i = getNextCollectionStartIndex(repetitionLevels, maxElementRepetitionLevel, i)) {
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

        long[] collectionIsValid = new long[wordsForBits(definitionLevels.length)];
        int position = fixedSizeCollections;
        setBits(collectionIsValid, 0, 0, position);
        int nullValuesCount = 0;
        for (int i = firstIndex; i < definitionLevels.length; i = getNextCollectionStartIndex(repetitionLevels, maxElementRepetitionLevel, i)) {
            if (definitionLevels[i] >= maxDefinitionLevel - 1) {
                boolean isNull = isOptionalFieldValueNull(definitionLevels[i], maxDefinitionLevel);
                nullValuesCount += isNull ? 1 : 0;
                if (!isNull) {
                    set(collectionIsValid, 0, position);
                }
                position++;
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
        return new BlockPositions(Optional.of(collectionIsValid), offsets.toIntArray());
    }

    /**
     * @return The size of the first collection, or 0 when the batch cannot have that shape
     */
    private static int getFixedCollectionSize(int maxElementRepetitionLevel, int[] repetitionLevels)
    {
        int length = repetitionLevels.length;
        if (length == 0 || repetitionLevels[0] >= maxElementRepetitionLevel) {
            return 0;
        }
        int size = 1;
        while (size < length && repetitionLevels[size] == maxElementRepetitionLevel) {
            size++;
        }
        if (length % size != 0) {
            return 0;
        }
        return size;
    }

    /**
     * @return The number of leading collections which are non-null, non-empty and have the given size
     */
    private static int countFixedSizeCollections(int maxDefinitionLevel, int maxElementRepetitionLevel, int[] definitionLevels, int[] repetitionLevels, int size, int collectionCount)
    {
        for (int collection = 0; collection < collectionCount; collection++) {
            int start = collection * size;
            if (repetitionLevels[start] >= maxElementRepetitionLevel) {
                // The previous collection continues here, so it is longer than the size
                return collection - 1;
            }
            // The collection has to be non-empty and not null. Whether its elements are null is irrelevant for offsets
            if (definitionLevels[start] <= maxDefinitionLevel) {
                return collection;
            }
            // Elements of nested collections have a higher repetition level and are not counted as elements of this one
            int mismatch = 0;
            for (int element = start + 1; element < start + size; element++) {
                mismatch |= repetitionLevels[element] ^ maxElementRepetitionLevel;
            }
            if (mismatch != 0) {
                return collection;
            }
        }
        return collectionCount;
    }

    public record BlockPositions(Optional<long[]> valueIsValid, int[] offsets) {}

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
