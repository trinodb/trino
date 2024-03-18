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
package io.varada.cloudstorage.hdfs;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.LifeCycleModule;
import io.airlift.configuration.ConfigurationFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorContext;

import java.lang.annotation.Annotation;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class HdfsCloudStorageModule
        implements Module
{
    private final String catalogName;
    private final ConnectorContext context;
    private final ConfigurationFactory configurationFactory;
    private final Class<? extends Annotation> annotation;
    private final boolean isHadoopEnabled;

    public HdfsCloudStorageModule(String catalogName,
                                  ConnectorContext context,
                                  ConfigurationFactory configurationFactory,
                                  Class<? extends Annotation> annotation,
                                  boolean isHadoopEnabled)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
        this.configurationFactory = requireNonNull(configurationFactory, "configurationFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
        this.isHadoopEnabled = isHadoopEnabled;
    }

    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, Key.get(HdfsCloudStorage.class, annotation));

        if (!isHadoopEnabled) {
            return;
        }

        Injector injector = Guice.createInjector(
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(ConfigurationFactory.class).toInstance(configurationFactory);
                        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
                        OpenTelemetry openTelemetry = context.getOpenTelemetry();
                        binder.bind(OpenTelemetry.class).toInstance(openTelemetry);
                        binder.bind(Tracer.class).toInstance(openTelemetry.getTracer("warp.cloud-vendor"));

                        binder.install(new LifeCycleModule());

                        FileSystemModule fileSystemModule = new FileSystemModule(catalogName, context.getNodeManager(), openTelemetry);
                        fileSystemModule.setConfigurationFactory(configurationFactory);
                        binder.install(fileSystemModule);
                    }
                });

        TrinoFileSystemFactory fileSystemFactory = injector.getInstance(Key.get(TrinoFileSystemFactory.class));
        binder.bind(HdfsCloudStorage.class).annotatedWith(annotation).toInstance(new HdfsCloudStorage(fileSystemFactory));
    }
}
