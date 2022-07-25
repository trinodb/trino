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
package io.trino.execution.scheduler;

import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

public interface TaskSourceFactory
{
    TaskSource create(
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
            LongConsumer getSplitTimeRecorder,
            Optional<int[]> bucketToPartitionMap,
            Optional<BucketNodeMap> bucketNodeMap);
}
