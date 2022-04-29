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

public class TotalReservationLowMemoryKiller
        implements LowMemoryKiller
{
    @Override
    public Optional<KillTarget> chooseTargetToKill(List<RunningQueryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Optional<QueryId> biggestQuery = Optional.empty();
        long maxMemory = 0;
        for (RunningQueryInfo query : runningQueries) {
            long bytesUsed = query.getMemoryReservation();
            if (bytesUsed > maxMemory) {
                biggestQuery = Optional.of(query.getQueryId());
                maxMemory = bytesUsed;
            }
        }
        return biggestQuery.map(KillTarget::wholeQuery);
    }
}
