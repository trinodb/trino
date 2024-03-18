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
package io.trino.plugin.varada.connector;

import com.google.inject.Module;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.RebindSafeMBeanServer;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.ProxiedConnectorInitializer;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingMetadata;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestingConnectorInitializer
        implements ProxiedConnectorInitializer
{
    @Override
    public List<Module> getModules(ConnectorContext context)
    {
        return Arrays.asList(
                new EventModule(),
                new MBeanModule(),
                // Taken from InternalHiveConnectorFactory (before https://github.com/prestosql/presto/commit/20851906ff48e82487967c2e16d6a96c574d59d6).
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.hive", "presto.plugin.hive"),
                new JsonModule(),
                binder -> {
                    newSetBinder(binder, Procedure.class);
                    binder.bind(DispatcherProxiedConnectorTransformer.class).to(TestingConnectorProxiedConnectorTransformer.class);
                    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                    binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(MetadataProvider.class).toInstance(context.getMetadataProvider());
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                });
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Connector connector = mock(Connector.class);
        when(connector.getPageSinkProvider()).thenReturn(mock(ConnectorPageSinkProvider.class));
        when(connector.getPageSourceProvider()).thenReturn(mock(ConnectorPageSourceProvider.class));
        when(connector.getSplitManager()).thenReturn(mock(ConnectorSplitManager.class));
        when(connector.getProcedures()).thenReturn(Collections.emptySet());
        when(connector.getMetadata(any(ConnectorSession.class), any(ConnectorTransactionHandle.class))).thenReturn(mock(ConnectorMetadata.class));
        when(connector.beginTransaction(any(IsolationLevel.class), anyBoolean(), anyBoolean())).thenReturn(mock(ConnectorTransactionHandle.class));

        ConnectorMetadata connectorMetadata = new TestingMetadata();
        when(connector.getMetadata(any(ConnectorSession.class), any(ConnectorTransactionHandle.class))).thenReturn(connectorMetadata);
        return connector;
    }
}
