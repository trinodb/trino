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

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.isOptionalFieldValueNull;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.wordsForBits;

public final class StructColumnReader
{
    private StructColumnReader() {}

    /**
     * Each struct has three variants of presence:
     * 1) Struct is not defined, because one of it's optional parent fields is null
     * 2) Struct is null
     * 3) Struct is defined and not empty.
     */
    public static RowBlockPositions calculateStructOffsets(
            Field field,
            int[] fieldDefinitionLevels,
            int[] fieldRepetitionLevels)
    {
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxRepetitionLevel = field.getRepetitionLevel();
        boolean required = field.isRequired();
        if (required) {
            int definedValuesCount = 0;
            for (int i = 0; i < fieldDefinitionLevels.length; i++) {
                if (fieldRepetitionLevels[i] <= maxRepetitionLevel) {
                    if (fieldDefinitionLevels[i] >= maxDefinitionLevel) {
                        // Struct is defined and not empty
                        definedValuesCount++;
                    }
                }
            }
            return new RowBlockPositions(Optional.empty(), definedValuesCount);
        }

        int nullValuesCount = 0;
        long[] structIsValid = new long[wordsForBits(fieldDefinitionLevels.length)];
        int position = 0;
        for (int i = 0; i < fieldDefinitionLevels.length; i++) {
            if (fieldRepetitionLevels[i] <= maxRepetitionLevel) {
                if (isOptionalFieldValueNull(fieldDefinitionLevels[i], maxDefinitionLevel)) {
                    // Struct is null
                    nullValuesCount++;
                    position++;
                }
                else if (fieldDefinitionLevels[i] >= maxDefinitionLevel) {
                    // Struct is defined and not empty
                    set(structIsValid, 0, position);
                    position++;
                }
            }
        }
        if (nullValuesCount == 0) {
            return new RowBlockPositions(Optional.empty(), position);
        }
        return new RowBlockPositions(Optional.of(structIsValid), position);
    }

    public record RowBlockPositions(Optional<long[]> valueIsValid, int positionsCount) {}
}
