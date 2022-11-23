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
package io.trino.plugin.blackhole;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class BlackHoleConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "blackhole";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        checkSpiVersion(context, this);

        ListeningScheduledExecutorService executorService = listeningDecorator(newSingleThreadScheduledExecutor(daemonThreadsNamed("blackhole")));
        return new BlackHoleConnector(
                new BlackHoleMetadata(),
                new BlackHoleSplitManager(),
                new BlackHolePageSourceProvider(executorService),
                new BlackHolePageSinkProvider(executorService),
                new BlackHoleNodePartitioningProvider(context.getTypeManager().getTypeOperators()),
                context.getTypeManager(),
                executorService);
    }
}
