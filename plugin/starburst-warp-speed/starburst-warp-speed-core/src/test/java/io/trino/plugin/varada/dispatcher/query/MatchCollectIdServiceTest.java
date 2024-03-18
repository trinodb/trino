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
package io.trino.plugin.varada.dispatcher.query;

import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MatchCollectIdServiceTest
{
    private static final int NUM_MATCH_COLLECT_IDS = 100;
    private static ExecutorService executor;

    private MatchCollectIdService matchCollectIdService;

    @BeforeAll
    public static void setUp()
    {
        executor = newCachedThreadPool();
    }

    @AfterAll
    public static void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @BeforeEach
    public void before()
    {
        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getMatchCollectNumIds()).thenReturn(NUM_MATCH_COLLECT_IDS);
        this.matchCollectIdService = new MatchCollectIdService(storageEngineConstants);
    }

    @Test
    public void testMatchCollectAllocFree()
    {
        int id;
        for (int i = 0; i < NUM_MATCH_COLLECT_IDS; i++) {
            id = matchCollectIdService.allocMatchCollectId();
            assertThat(id).isNotEqualTo(MatchCollectIdService.INVALID_ID);
        }
        id = matchCollectIdService.allocMatchCollectId();
        assertThat(id).isEqualTo(MatchCollectIdService.INVALID_ID);
        assertThat(matchCollectIdService.getNumFreeIds()).isEqualTo(0);
        for (int i = 0; i < 2; i++) {
            matchCollectIdService.freeMatchCollectId(i);
        }
        assertThat(matchCollectIdService.getNumFreeIds()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            id = matchCollectIdService.allocMatchCollectId();
            assertThat(id).isNotEqualTo(MatchCollectIdService.INVALID_ID);
        }
        id = matchCollectIdService.allocMatchCollectId();
        assertThat(id).isEqualTo(MatchCollectIdService.INVALID_ID);
        assertThat(matchCollectIdService.getNumFreeIds()).isEqualTo(0);
    }

    @Test
    public void testParallelAllocateAndThenFree()
            throws CancellationException, ExecutionException, InterruptedException
    {
        int numberOfThreads = 3;
        CountDownLatch allocLatch = new CountDownLatch(numberOfThreads);
        CountDownLatch freeLatch = new CountDownLatch(numberOfThreads);
        List<Future<Integer>> futures = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            futures.add(executor.submit(() -> {
                int[] ids = new int[NUM_MATCH_COLLECT_IDS];
                int numSuccessfulAllocations = 0;

                allocLatch.countDown();
                allocLatch.await();
                int matchCollectId = matchCollectIdService.allocMatchCollectId();
                while (matchCollectId != MatchCollectIdService.INVALID_ID) {
                    ids[numSuccessfulAllocations] = matchCollectId;
                    numSuccessfulAllocations++;
                    matchCollectId = matchCollectIdService.allocMatchCollectId();
                }

                freeLatch.countDown();
                freeLatch.await();
                IntStream.range(0, numSuccessfulAllocations)
                        .forEach(j -> matchCollectIdService.freeMatchCollectId(ids[j]));

                return numSuccessfulAllocations;
            }));
        }

        int totalAllocations = 0;
        for (Future<Integer> future : futures) {
            totalAllocations += future.get();
        }
        assertThat(totalAllocations).isEqualTo(NUM_MATCH_COLLECT_IDS);
        assertThat(matchCollectIdService.getNumFreeIds()).isEqualTo(NUM_MATCH_COLLECT_IDS);
    }

    @Test
    public void testAllocateParallelToFree()
            throws CancellationException, ExecutionException, InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(2);
        Future<?> freeThread = executor.submit(() -> {
            int[] ids = new int[NUM_MATCH_COLLECT_IDS / 2];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = matchCollectIdService.allocMatchCollectId();
            }
            Arrays.stream(ids).forEach(id -> assertThat(id).isNotEqualTo(MatchCollectIdService.INVALID_ID));
            latch.countDown();
            latch.await();

            Arrays.stream(ids).forEach(id -> matchCollectIdService.freeMatchCollectId(id));
            return null;
        });
        Future<?> allocThread = executor.submit(() -> {
            int[] ids = new int[NUM_MATCH_COLLECT_IDS];
            int numSuccessfulAllocations = 0;
            latch.countDown();
            latch.await();

            int matchCollectId = matchCollectIdService.allocMatchCollectId();
            while (matchCollectId != MatchCollectIdService.INVALID_ID) {
                ids[numSuccessfulAllocations] = matchCollectId;
                numSuccessfulAllocations++;
                matchCollectId = matchCollectIdService.allocMatchCollectId();
            }

            IntStream.range(0, numSuccessfulAllocations)
                    .forEach(j -> matchCollectIdService.freeMatchCollectId(ids[j]));
            return null;
        });

        freeThread.get();
        allocThread.get();
        assertThat(matchCollectIdService.getNumFreeIds()).isEqualTo(NUM_MATCH_COLLECT_IDS);
    }
}
