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
package io.trino.lance.file.v2.reader;

import io.trino.lance.file.v2.metadata.RepDefLayer;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.lance.file.v2.metadata.RepDefLayer.ALL_VALID_ITEM;
import static io.trino.lance.file.v2.metadata.RepDefLayer.NULLABLE_ITEM;

// Lance uses repetition/definition levels similar to Parquet for nested data.
// Key difference is that Lance uses a layered system with 6 RepDefLayer types:
// - ALL_VALID_ITEM/ALL_VALID_LIST: no nulls/empties (0 def levels)
// - NULLABLE_ITEM/NULLABLE_LIST: nulls only (1 def level)
// - EMPTYABLE_LIST: empty lists only (1 def level)
// - NULLABLE_AND_EMPTYABLE_LIST: both null and empty lists (2 def levels)
public class BaseUnraveler
        implements RepetitionDefinitionUnraveler
{
    private final int[] repetitionLevels;
    private final int[] definitionLevels;
    private final RepDefLayer[] layers;
    private final int[] levelsToRepetition;

    private int currentDefinitionLevel;
    private int currentRepetitionLevel;
    private int currentLayer;

    public BaseUnraveler(int[] repetitionLevels, int[] definitionLevels, RepDefLayer[] layers)
    {
        this.repetitionLevels = repetitionLevels.length == 0 ? null : repetitionLevels;
        this.definitionLevels = definitionLevels.length == 0 ? null : definitionLevels;
        this.layers = layers;

        IntArrayList levelsToRepetitionBuilder = new IntArrayList();
        int repetitionCounter = 0;
        levelsToRepetitionBuilder.add(0);
        for (RepDefLayer layer : layers) {
            switch (layer) {
                case ALL_VALID_ITEM, ALL_VALID_LIST -> {}
                case NULLABLE_ITEM -> levelsToRepetitionBuilder.add(repetitionCounter);
                case NULLABLE_LIST, EMPTYABLE_LIST -> {
                    repetitionCounter++;
                    levelsToRepetitionBuilder.add(repetitionCounter);
                }
                case NULLABLE_AND_EMPTYABLE_LIST -> {
                    repetitionCounter++;
                    levelsToRepetitionBuilder.add(repetitionCounter);
                    levelsToRepetitionBuilder.add(repetitionCounter);
                }
            }
        }
        this.levelsToRepetition = levelsToRepetitionBuilder.toIntArray();
        this.currentDefinitionLevel = 0;
        this.currentRepetitionLevel = 0;
        this.currentLayer = 0;
    }

    @Override
    public boolean isAllValid()
    {
        return layers[currentLayer].isAllValid();
    }

    @Override
    public void skipValidity()
    {
        verify(layers[currentLayer] == ALL_VALID_ITEM);
        currentLayer++;
    }

    @Override
    public long maxLists()
    {
        checkArgument(layers[currentLayer] != NULLABLE_ITEM, "Current layer must not be a struct or primitive");
        return repetitionLevels.length;
    }

    @Override
    public BlockPositions calculateOffsets()
    {
        int validLevel = currentDefinitionLevel;
        int nullLevel;
        int emptyLevel;

        RepDefLayer currentMeaning = layers[currentLayer];
        switch (currentMeaning) {
            case NULLABLE_LIST -> {
                currentDefinitionLevel++;
                nullLevel = validLevel + 1;
                emptyLevel = 0;
            }
            case EMPTYABLE_LIST -> {
                currentDefinitionLevel++;
                nullLevel = 0;
                emptyLevel = validLevel + 1;
            }
            case NULLABLE_AND_EMPTYABLE_LIST -> {
                currentDefinitionLevel += 2;
                nullLevel = validLevel + 1;
                emptyLevel = validLevel + 2;
            }
            case ALL_VALID_LIST -> {
                nullLevel = 0;
                emptyLevel = 0;
            }
            default -> throw new IllegalStateException("Unexpected layer for list");
        }
        currentLayer++;

        int maxLevel = Math.max(nullLevel, emptyLevel);
        int upperNull = maxLevel;

        for (int i = currentLayer; i < layers.length; i++) {
            RepDefLayer level = layers[i];
            switch (level) {
                case NULLABLE_ITEM -> maxLevel++;
                case ALL_VALID_ITEM -> { /* noop */ }
                default -> i = layers.length;
            }
        }

        IntArrayList offsets = new IntArrayList();
        int currentOffset = 0;

        if (definitionLevels != null) {
            BooleanArrayList nullList = new BooleanArrayList();

            if (repetitionLevels.length != definitionLevels.length) {
                throw new IllegalStateException("Rep and def levels must have same length");
            }

            for (int i = 0; i < repetitionLevels.length; i++) {
                int repetitionLevel = repetitionLevels[i];

                if (repetitionLevel > currentRepetitionLevel) {
                    int definitionLevel = definitionLevels[i];
                    if (definitionLevel == 0) {
                        // Valid list
                        offsets.add(currentOffset);
                        currentOffset += 1;
                        nullList.add(false);
                    }
                    else if (definitionLevel > maxLevel) {
                        // Invisible at this level
                    }
                    else if (definitionLevel == nullLevel || definitionLevel > upperNull) {
                        // Null list
                        offsets.add(currentOffset);
                        nullList.add(true);
                    }
                    else if (definitionLevel == emptyLevel) {
                        // Empty list
                        offsets.add(currentOffset);
                        nullList.add(false);
                    }
                    else {
                        // New valid list starting with null item
                        offsets.add(currentOffset);
                        currentOffset += 1;
                        nullList.add(false);
                    }
                }
                else if (repetitionLevel == currentRepetitionLevel) {
                    currentOffset += 1;
                }
            }

            offsets.add(currentOffset);
            currentRepetitionLevel++;
            return new BlockPositions(Optional.of(nullList.toBooleanArray()), offsets.toIntArray());
        }
        else {
            for (int repetitionLevel : repetitionLevels) {
                if (repetitionLevel > currentRepetitionLevel) {
                    offsets.add(currentOffset);
                    currentOffset += 1;
                }
                else if (repetitionLevel == currentRepetitionLevel) {
                    currentOffset += 1;
                }
            }

            offsets.add(currentOffset);
            currentRepetitionLevel++;
            return new BlockPositions(Optional.empty(), offsets.toIntArray());
        }
    }

    @Override
    public Optional<boolean[]> calculateNulls()
    {
        if (layers[currentLayer] == ALL_VALID_ITEM) {
            currentLayer++;
            return Optional.empty();
        }
        BooleanArrayList nullList = new BooleanArrayList();

        for (int level : definitionLevels) {
            if (levelsToRepetition[level] <= currentRepetitionLevel) {
                nullList.add(level > currentDefinitionLevel);
            }
        }
        currentLayer++;
        currentDefinitionLevel++;
        return Optional.of(nullList.toBooleanArray());
    }
}
