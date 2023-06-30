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

import org.testng.annotations.DataProvider;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static io.trino.testing.DataProviders.toDataProvider;

enum NullsProvider
{
    NO_NULLS {
        @Override
        Optional<boolean[]> getNulls(int positionCount)
        {
            return Optional.empty();
        }
    },
    NO_NULLS_WITH_MAY_HAVE_NULL {
        @Override
        Optional<boolean[]> getNulls(int positionCount)
        {
            return Optional.of(new boolean[positionCount]);
        }
    },
    ALL_NULLS {
        @Override
        Optional<boolean[]> getNulls(int positionCount)
        {
            boolean[] nulls = new boolean[positionCount];
            Arrays.fill(nulls, true);
            return Optional.of(nulls);
        }
    },
    RANDOM_NULLS {
        @Override
        Optional<boolean[]> getNulls(int positionCount)
        {
            boolean[] nulls = new boolean[positionCount];
            for (int i = 0; i < positionCount; i++) {
                nulls[i] = RANDOM.nextBoolean();
            }
            return Optional.of(nulls);
        }
    },
    GROUPED_NULLS {
        @Override
        Optional<boolean[]> getNulls(int positionCount)
        {
            boolean[] nulls = new boolean[positionCount];
            int maxGroupSize = 23;
            int position = 0;
            while (position < positionCount) {
                int remaining = positionCount - position;
                int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
                Arrays.fill(nulls, position, position + groupSize, RANDOM.nextBoolean());
                position += groupSize;
            }
            return Optional.of(nulls);
        }
    };

    private static final Random RANDOM = new Random(42);

    abstract Optional<boolean[]> getNulls(int positionCount);

    @DataProvider
    public static Object[][] nullsProviders()
    {
        return Stream.of(NullsProvider.values())
                .collect(toDataProvider());
    }
}
