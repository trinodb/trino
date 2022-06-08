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
package io.trino.sql.planner;

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.RoundRobinBucketFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSystemPartitioningHandle
{
    private static final Page DUMMY_PAGE = new Page(10_000_000);

    @Test(dataProvider = "testRoundRobinBucketFunctionDataProvider")
    public void testRoundRobinBucketFunction(int bucketCount, int counterStart, int firstExpectedBucket, int testSequenceLength)
    {
        RoundRobinBucketFunction calledOneByOne = new RoundRobinBucketFunction(bucketCount);
        calledOneByOne.setCounter(counterStart);

        List<Integer> actual = new ArrayList<>();
        for (int i = 0; i < testSequenceLength; i++) {
            actual.add(calledOneByOne.getBucket(DUMMY_PAGE, testSequenceLength));
        }
        int calledOneByOneCounter = calledOneByOne.getCounter();

        List<Integer> expected = IntStream.iterate(firstExpectedBucket, bucket -> (bucket + 1) % bucketCount)
                .limit(testSequenceLength)
                .boxed()
                .toList();

        assertThat(actual)
                .isEqualTo(expected);

        RoundRobinBucketFunction calledBatched = new RoundRobinBucketFunction(bucketCount);
        calledBatched.setCounter(counterStart);

        int[] batchedActual = new int[testSequenceLength];
        calledBatched.getBuckets(DUMMY_PAGE, 0, testSequenceLength, batchedActual);
        int calledBatchedCounter = calledBatched.getCounter();

        // Batched version should produce same results
        assertThat(Ints.asList(batchedActual))
                .isEqualTo(expected);

        assertThat(calledBatchedCounter)
                .isEqualTo(calledOneByOneCounter);
    }

    @DataProvider
    public static Object[][] testRoundRobinBucketFunctionDataProvider()
    {
        return new Object[][] {
                {4, 0, 0, 5},
                {4, 0, 0, 42},
                {4, RoundRobinBucketFunction.WRAP_AROUND - 3, 0, 42},
        };
    }
}
