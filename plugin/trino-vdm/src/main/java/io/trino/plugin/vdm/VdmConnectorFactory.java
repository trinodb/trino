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
package io.trino.plugin.vdm;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.NodeManager;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.metastore.HetuMetastore;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * vdm connector factory
 * vdm connector factory
 *
 * @since 2023-04-06
 */
public class VdmConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final ClassLoader classLoader;

    /**
     * vdm connector factory
     *
     * @param name vdm type
     * @param classLoader classLoader
     */
    public VdmConnectorFactory(String name, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        NodeManager nodeManager = context.getNodeManager();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            VdmName vdm = new VdmName(catalogName, name);

            Bootstrap app = new Bootstrap(binder -> {
                binder.bind(VdmName.class).toInstance(vdm);
                binder.bind(HetuMetastore.class).toInstance(context.getHetuMetastore());
                binder.bind(NodeVersion.class).toInstance(new NodeVersion(nodeManager.getCurrentNode().getVersion()));
            },
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new VdmModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(VdmConnector.class);
        }
        catch (Exception ex) {
            throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }
    }
}
