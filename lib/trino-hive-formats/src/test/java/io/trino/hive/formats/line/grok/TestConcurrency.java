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
package io.trino.hive.formats.line.grok;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class TestConcurrency
{
    /**
     * We will test this by setting up two threads, asserting on the hash values
     * the instances generate for each thread
     *
     * @throws InterruptedException throws InterruptedException
     * @throws ExecutionException throws ExecutionException
     */
    @Test
    public void test_001_concurrent_match()
            throws InterruptedException, ExecutionException
    {
        // Setup callable method that reports the hashcode for the thread
        int threadCount = 2;
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call()
            {
                return Match.getInstance().hashCode();
            }
        };

        // Create n tasks to execute
        List<Callable<Integer>> tasks = Collections.nCopies(threadCount, task);

        // Execute the task for both tasks
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = es.invokeAll(tasks);
        int hash1 = futures.get(0).get();
        int hash2 = futures.get(1).get();

        // The two hashcodes must NOT be equal
        assertThat(hash1).isNotEqualTo(hash2);
    }

    @Test
    public void test_002_match_within_instance()
    {
        // Verify that the instances are equal for the same thread
        Match m1 = Match.getInstance();
        Match m2 = Match.getInstance();
        assertThat(m1).isEqualTo(m2);
    }
}
