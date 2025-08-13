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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CompositeUnraveler
        implements RepetitionDefinitionUnraveler
{
    private final List<RepetitionDefinitionUnraveler> unravelers;

    public CompositeUnraveler(List<RepetitionDefinitionUnraveler> unravelers)
    {
        this.unravelers = requireNonNull(unravelers, "unravelers is null");
    }

    @Override
    public BlockPositions calculateOffsets()
    {
        boolean allValid = isAllValid();
        IntArrayList offsetBuffer = new IntArrayList();
        BooleanArrayList nullBuffer = new BooleanArrayList();
        for (RepetitionDefinitionUnraveler unraveler : unravelers) {
            BlockPositions positions = unraveler.calculateOffsets();
            if (offsetBuffer.isEmpty()) {
                offsetBuffer.addAll(IntList.of(positions.offsets()));
            }
            else {
                int lastOffset = offsetBuffer.popInt();
                for (int i = 0; i < positions.offsets().length; i++) {
                    offsetBuffer.add(positions.offsets()[i] + lastOffset);
                }
            }
            if (positions.nulls().isPresent()) {
                nullBuffer.addAll(BooleanList.of(positions.nulls().get()));
            }
            else if (!allValid) {
                for (int i = 0; i < positions.offsets().length - 1; i++) {
                    nullBuffer.add(false);
                }
            }
        }
        if (allValid) {
            return new BlockPositions(Optional.empty(), offsetBuffer.toIntArray());
        }
        return new BlockPositions(Optional.of(nullBuffer.toBooleanArray()), offsetBuffer.toIntArray());
    }

    @Override
    public Optional<boolean[]> calculateNulls()
    {
        if (isAllValid()) {
            skipValidity();
            return Optional.empty();
        }
        else {
            BooleanArrayList validity = unravelers.stream()
                    .map(unraveler -> unraveler.calculateNulls().orElseThrow())
                    .collect(BooleanArrayList::new,
                            (buffer, array) -> {
                                for (boolean value : array) {
                                    buffer.add(value);
                                }
                            },
                            BooleanArrayList::addAll);
            return Optional.of(validity.toBooleanArray());
        }
    }

    @Override
    public boolean isAllValid()
    {
        return unravelers.stream().allMatch(RepetitionDefinitionUnraveler::isAllValid);
    }

    @Override
    public void skipValidity()
    {
        unravelers.forEach(RepetitionDefinitionUnraveler::skipValidity);
    }

    @Override
    public long maxLists()
    {
        return unravelers.stream().map(RepetitionDefinitionUnraveler::maxLists).mapToLong(Long::longValue).sum();
    }
}
