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
package io.trino.plugin.hive.metastore.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.google.inject.Inject;

import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public class DefaultGlueColumnStatisticsProviderFactory
        implements GlueColumnStatisticsProviderFactory
{
    private final Executor statisticsReadExecutor;
    private final Executor statisticsWriteExecutor;

    @Inject
    public DefaultGlueColumnStatisticsProviderFactory(
            @ForGlueColumnStatisticsRead Executor statisticsReadExecutor,
            @ForGlueColumnStatisticsWrite Executor statisticsWriteExecutor)
    {
        this.statisticsReadExecutor = requireNonNull(statisticsReadExecutor, "statisticsReadExecutor is null");
        this.statisticsWriteExecutor = requireNonNull(statisticsWriteExecutor, "statisticsWriteExecutor is null");
    }

    @Override
    public GlueColumnStatisticsProvider createGlueColumnStatisticsProvider(AWSGlueAsync glueClient, GlueMetastoreStats stats)
    {
        return new DefaultGlueColumnStatisticsProvider(
                glueClient,
                statisticsReadExecutor,
                statisticsWriteExecutor,
                stats);
    }
}
