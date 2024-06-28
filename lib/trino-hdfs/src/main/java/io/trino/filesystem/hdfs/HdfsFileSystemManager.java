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
package io.trino.filesystem.hdfs;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsModule;
import io.trino.hdfs.authentication.HdfsAuthenticationModule;
import io.trino.hdfs.azure.HiveAzureModule;
import io.trino.hdfs.cos.HiveCosModule;
import io.trino.hdfs.gcs.HiveGcsModule;
import io.trino.hdfs.s3.HiveS3Module;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import org.weakref.jmx.guice.MBeanModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public final class HdfsFileSystemManager
{
    private final Bootstrap bootstrap;
    private LifeCycleManager lifecycleManager;

    public HdfsFileSystemManager(
            Map<String, String> config,
            boolean azureEnabled,
            boolean gcsEnabled,
            boolean s3Enabled,
            String catalogName,
            NodeManager nodeManager,
            OpenTelemetry openTelemetry)
    {
        List<Module> modules = new ArrayList<>();

        modules.add(new MBeanModule());
        modules.add(new MBeanServerModule());
        modules.add(new ConnectorObjectNameGeneratorModule("", ""));

        modules.add(new HdfsFileSystemModule());
        modules.add(new HdfsModule());
        modules.add(new HdfsAuthenticationModule());
        modules.add(new HiveCosModule());
        modules.add(binder -> {
            binder.bind(NodeManager.class).toInstance(nodeManager);
            binder.bind(OpenTelemetry.class).toInstance(openTelemetry);
            binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
        });

        if (azureEnabled) {
            modules.add(new HiveAzureModule());
        }
        if (gcsEnabled) {
            modules.add(new HiveGcsModule());
        }
        if (s3Enabled) {
            modules.add(new HiveS3Module());
        }

        bootstrap = new Bootstrap(modules)
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(Map.of())
                .setOptionalConfigurationProperties(config);
    }

    public Set<String> configure()
    {
        return bootstrap.configure();
    }

    public TrinoFileSystemFactory create()
    {
        checkState(lifecycleManager == null, "Already created");
        Injector injector = bootstrap.initialize();
        lifecycleManager = injector.getInstance(LifeCycleManager.class);
        return injector.getInstance(HdfsFileSystemFactory.class);
    }

    public void stop()
    {
        if (lifecycleManager != null) {
            lifecycleManager.stop();
        }
    }
}
