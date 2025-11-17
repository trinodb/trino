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
package io.trino.memory;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryState;
import io.trino.spi.QueryId;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.execution.QueryState.RUNNING;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ClusterMemoryLeakDetector
{
    private static final Logger log = Logger.get(ClusterMemoryLeakDetector.class);

    // It may take some time to remove a query's memory reservations from the worker nodes, that's why
    // we check to see whether some time has passed after the query finishes to claim that it is leaked.
    private static final int DEFAULT_LEAK_CLAIM_DELTA_SEC = 60;

    @GuardedBy("this")
    private Set<QueryId> leakedQueries;

    /**
     * @param executionInfoSupplier Provided QueryId returns a QueryExecution if the query is still tracked by the coordinator.
     * @param queryMemoryReservations The memory reservations of queries in the cluster memory pool.
     */
    void checkForMemoryLeaks(Function<QueryId, Optional<QueryExecution>> executionInfoSupplier, Map<QueryId, Long> queryMemoryReservations)
    {
        requireNonNull(queryMemoryReservations);

        ImmutableSet.Builder<QueryId> leakedQueriesBuilder = ImmutableSet.builder();
        queryMemoryReservations.forEach((queryId, reservation) -> {
            if (reservation > 0) {
                if (isLeaked(executionInfoSupplier.apply(queryId))) {
                    leakedQueriesBuilder.add(queryId);
                }
            }
        });

        Set<QueryId> leakedQueryReservations = leakedQueriesBuilder.build();
        if (!leakedQueryReservations.isEmpty()) {
            log.debug("Memory leak detected. The following queries are already finished, " +
                    "but they have memory reservations on some worker node(s): %s", leakedQueryReservations);
        }

        synchronized (this) {
            leakedQueries = ImmutableSet.copyOf(leakedQueryReservations);
        }
    }

    private static boolean isLeaked(Optional<QueryExecution> execution)
    {
        if (execution.isEmpty()) {
            // We have a memory reservation but query isn't tracked
            return true;
        }

        Optional<Instant> queryEndTime = execution.orElseThrow().getEndTime();
        QueryState state = execution.orElseThrow().getState();

        if (state == RUNNING || queryEndTime.isEmpty()) {
            return false;
        }

        return queryEndTime.orElseThrow().plusSeconds(DEFAULT_LEAK_CLAIM_DELTA_SEC).isBefore(now());
    }

    synchronized boolean wasQueryPossiblyLeaked(QueryId queryId)
    {
        return leakedQueries.contains(queryId);
    }

    synchronized int getNumberOfLeakedQueries()
    {
        return leakedQueries.size();
    }
}
