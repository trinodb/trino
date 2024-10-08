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
package io.trino.spooling.filesystem;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemModule;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemModule;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.protocol.SpoolingManagerContext;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.plugin.base.ClosingBinder.closingBinder;

public class FileSystemSpoolingModule
        extends AbstractConfigurationAwareModule
{
    private final boolean coordinator;

    public FileSystemSpoolingModule(boolean coordinator)
    {
        this.coordinator = coordinator;
    }

    @Override
    protected void setup(Binder binder)
    {
        FileSystemSpoolingConfig config = buildConfigObject(FileSystemSpoolingConfig.class);
        var factories = newMapBinder(binder, String.class, TrinoFileSystemFactory.class);
        if (config.isAzureEnabled()) {
            install(new AzureFileSystemModule());
            factories.addBinding("abfs").to(AzureFileSystemFactory.class);
        }
        if (config.isS3Enabled()) {
            install(new S3FileSystemModule());
            factories.addBinding("s3").to(S3FileSystemFactory.class);
        }
        if (config.isGcsEnabled()) {
            install(new GcsFileSystemModule());
            factories.addBinding("gs").to(GcsFileSystemFactory.class);
        }
        binder.bind(SpoolingManager.class).to(FileSystemSpoolingManager.class).in(Scopes.SINGLETON);

        if (coordinator) {
            binder.bind(FileSystemSegmentPruner.class).asEagerSingleton();
            binder.bind(ScheduledExecutorService.class)
                    .annotatedWith(ForSegmentPruner.class)
                    .toInstance(Executors.newScheduledThreadPool(1, threadsNamed("segment-pruner-%d")));

            closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForSegmentPruner.class));
        }
    }

    @Provides
    @Singleton
    public TrinoFileSystemFactory createFileSystemFactory(
            Map<String, TrinoFileSystemFactory> factories,
            SpoolingManagerContext context)
    {
        Function<Location, TrinoFileSystemFactory> loader = location -> location.scheme()
                .map(factories::get)
                .orElseThrow(() -> new IllegalArgumentException("No factory for location: " + location));

        return new TracingFileSystemFactory(context.getTracer(), new SwitchingFileSystemFactory(loader));
    }
}
