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
package io.trino.filesystem.manager;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemModule;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemModule;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.NodeManager;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class FileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final NodeManager nodeManager;
    private final OpenTelemetry openTelemetry;

    public FileSystemModule(String catalogName, NodeManager nodeManager, OpenTelemetry openTelemetry)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        FileSystemConfig config = buildConfigObject(FileSystemConfig.class);

        newOptionalBinder(binder, HdfsFileSystemLoader.class);

        if (config.isHadoopEnabled()) {
            HdfsFileSystemLoader loader = new HdfsFileSystemLoader(
                    getProperties(),
                    !config.isNativeAzureEnabled(),
                    !config.isNativeGcsEnabled(),
                    !config.isNativeS3Enabled(),
                    catalogName,
                    nodeManager,
                    openTelemetry);

            loader.configure().forEach(this::consumeProperty);
            binder.bind(HdfsFileSystemLoader.class).toInstance(loader);
        }

        var factories = newMapBinder(binder, String.class, TrinoFileSystemFactory.class);

        if (config.isNativeAzureEnabled()) {
            install(new AzureFileSystemModule());
            factories.addBinding("abfs").to(AzureFileSystemFactory.class);
            factories.addBinding("abfss").to(AzureFileSystemFactory.class);
        }

        if (config.isNativeS3Enabled()) {
            install(new S3FileSystemModule());
            factories.addBinding("s3").to(S3FileSystemFactory.class);
            factories.addBinding("s3a").to(S3FileSystemFactory.class);
            factories.addBinding("s3n").to(S3FileSystemFactory.class);
        }

        if (config.isNativeGcsEnabled()) {
            install(new GcsFileSystemModule());
            factories.addBinding("gs").to(GcsFileSystemFactory.class);
        }
    }

    @Provides
    @Singleton
    public TrinoFileSystemFactory createFileSystemFactory(
            Optional<HdfsFileSystemLoader> hdfsFileSystemLoader,
            LifeCycleManager lifeCycleManager,
            Map<String, TrinoFileSystemFactory> factories,
            Tracer tracer)
    {
        Optional<TrinoFileSystemFactory> hdfsFactory = hdfsFileSystemLoader.map(HdfsFileSystemLoader::create);
        hdfsFactory.ifPresent(lifeCycleManager::addInstance);

        TrinoFileSystemFactory delegate = new SwitchingFileSystemFactory(hdfsFactory, factories);
        return new TracingFileSystemFactory(tracer, delegate);
    }
}
