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
package io.trino.parquet.writer;

import java.util.Optional;
import java.util.Random;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.wordsForBits;

enum NullsProvider
{
    NO_NULLS {
        @Override
        Optional<long[]> getValidities(int positionCount)
        {
            return Optional.empty();
        }
    },
    NO_NULLS_WITH_MAY_HAVE_NULL {
        @Override
        Optional<long[]> getValidities(int positionCount)
        {
            long[] valueIsValid = allocateWords(positionCount, true);
            return Optional.of(valueIsValid);
        }
    },
    ALL_NULLS {
        @Override
        Optional<long[]> getValidities(int positionCount)
        {
            return Optional.of(new long[wordsForBits(positionCount)]);
        }
    },
    RANDOM_NULLS {
        @Override
        Optional<long[]> getValidities(int positionCount)
        {
            long[] valueIsValid = new long[wordsForBits(positionCount)];
            for (int i = 0; i < positionCount; i++) {
                if (!RANDOM.nextBoolean()) {
                    set(valueIsValid, 0, i);
                }
            }
            return Optional.of(valueIsValid);
        }
    },
    GROUPED_NULLS {
        @Override
        Optional<long[]> getValidities(int positionCount)
        {
            long[] valueIsValid = new long[wordsForBits(positionCount)];
            int maxGroupSize = 23;
            int position = 0;
            while (position < positionCount) {
                int remaining = positionCount - position;
                int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
                boolean valid = !RANDOM.nextBoolean();
                if (valid) {
                    for (int i = position; i < position + groupSize; i++) {
                        set(valueIsValid, 0, i);
                    }
                }
                position += groupSize;
            }
            return Optional.of(valueIsValid);
        }
    };

    private static final Random RANDOM = new Random(42);

    abstract Optional<long[]> getValidities(int positionCount);

    Optional<long[]> getValidities(int positionCount, Optional<long[]> forcedValidities)
    {
        Optional<long[]> validities = getValidities(positionCount);
        if (forcedValidities.isEmpty()) {
            return validities;
        }
        if (validities.isEmpty()) {
            return forcedValidities;
        }

        long[] validity = validities.get();
        long[] forcedValidity = forcedValidities.get();
        long[] combined = new long[wordsForBits(positionCount)];
        for (int i = 0; i < positionCount; i++) {
            if (isSet(validity, 0, i) && isSet(forcedValidity, 0, i)) {
                set(combined, 0, i);
            }
        }
        return Optional.of(combined);
    }
}
