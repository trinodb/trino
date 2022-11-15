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

import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.ForStatsProvider;
import io.trino.cost.SimpleTableStatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.cost.TimingTableStatsProvider;
import io.trino.metadata.Metadata;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class QueryTableStatsProviderFactory
{
    private final Metadata metadata;
    private final ExecutorService executor;
    private final Optional<Long> timeoutMillis;

    @Inject
    public QueryTableStatsProviderFactory(Metadata metadata, @ForStatsProvider ExecutorService executor, OptimizerConfig optimizerConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.timeoutMillis = optimizerConfig.getConnectorStatsProvisioningTimeout().map(Duration::toMillis);
    }

    public TableStatsProvider create(Session session)
    {
        TableStatsProvider tableStatsProvider = new SimpleTableStatsProvider(metadata, session);
        if (timeoutMillis.isPresent()) {
            tableStatsProvider = new TimingTableStatsProvider(tableStatsProvider, session, executor, timeoutMillis.get());
        }
        tableStatsProvider = new CachingTableStatsProvider(tableStatsProvider);
        return tableStatsProvider;
    }
}
