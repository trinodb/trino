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

import io.trino.spi.QueryId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface LowMemoryKiller
{
    Optional<QueryId> chooseQueryToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes);

    class QueryMemoryInfo
    {
        private final QueryId queryId;
        private final long memoryReservation;

        public QueryMemoryInfo(QueryId queryId, long memoryReservation)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.memoryReservation = memoryReservation;
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public long getMemoryReservation()
        {
            return memoryReservation;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("memoryReservation", memoryReservation)
                    .toString();
        }
    }
}
