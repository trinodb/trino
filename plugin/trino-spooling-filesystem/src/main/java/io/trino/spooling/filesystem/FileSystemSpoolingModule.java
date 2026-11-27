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
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.spool.SpoolingManagerContext;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class FileSystemSpoolingModule
        extends AbstractConfigurationAwareModule
{
    private final boolean coordinator;

    private static final Duration ROLE_SESSION_DURATION_MARGIN = Duration.ofMinutes(5);

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
            checkDirectAccessTtl(config, buildConfigObject(S3FileSystemConfig.class));
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

        switch (config.getLayout()) {
            case SIMPLE -> binder.bind(FileSystemLayout.class).to(SimpleFileSystemLayout.class);
            case PARTITIONED -> {
                configBinder(binder).bindConfig(PartitionedLayoutConfig.class);
                binder.bind(FileSystemLayout.class).to(PartitionedFileSystemLayout.class);
            }
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

    private static void checkDirectAccessTtl(FileSystemSpoolingConfig spoolingConfig, S3FileSystemConfig s3Config)
    {
        if (!s3Config.isPresignedUriLifetimeValidationEnabled()) {
            return;
        }
        var authType = s3Config.getAuthType();
        if (authType != S3FileSystemConfig.S3AuthType.WEB_IDENTITY && authType != S3FileSystemConfig.S3AuthType.IAM_ROLE) {
            // Session duration settings only apply to WEB_IDENTITY or IAM_ROLE auth types, nothing to check.
            return;
        }
        Duration directAccessTtl = spoolingConfig.getDirectAccessTtl().toJavaTime();
        Duration roleSessionDuration = s3Config.getRoleSessionDuration().toJavaTime();
        checkArgument(roleSessionDuration.compareTo(directAccessTtl.plus(ROLE_SESSION_DURATION_MARGIN)) >= 0,
                "fs.segment.direct.ttl (%s) must be no longer than s3.role-session-duration (%s) minus %s when s3.presigned-uri.lifetime-validation.enabled is set. " +
                        "AWS limits sessions of chained roles, such as s3.iam-role assumed with web identity credentials, to 1h, so use a shorter fs.segment.direct.ttl instead of a longer session",
                spoolingConfig.getDirectAccessTtl(),
                s3Config.getRoleSessionDuration(),
                io.airlift.units.Duration.succinctDuration(ROLE_SESSION_DURATION_MARGIN.toMinutes(), TimeUnit.MINUTES));
    }
}
