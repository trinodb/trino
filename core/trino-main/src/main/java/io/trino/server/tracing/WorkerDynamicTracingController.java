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
package io.trino.server.tracing;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.sdk.internal.ScopeConfigurator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.internal.SdkTracerProviderUtil;
import io.opentelemetry.sdk.trace.internal.TracerConfig;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class WorkerDynamicTracingController
        implements DynamicTracingController
{
    private static final Logger log = Logger.get(WorkerDynamicTracingController.class);

    private final AtomicBoolean exportEnabled;
    private final SdkTracerProvider tracerProvider;
    private static final AtomicReference<Instant> since = new AtomicReference<>(Instant.now());

    @Inject
    public WorkerDynamicTracingController(SdkTracerProvider tracerProvider, DynamicTracingConfig config)
    {
        this.tracerProvider = requireNonNull(tracerProvider, "tracerProvider is null");
        this.exportEnabled = new AtomicBoolean(requireNonNull(config, "config is null").isExportEnabled());

        log.info("Exporting OpenTelemetry traces is now %s", exportEnabled.get() ? "enabled" : "disabled");
    }

    @Override
    public TracingStatus enableExport()
    {
        if (exportEnabled.compareAndSet(false, true)) {
            since.set(Instant.now());
            log.info("Enabling exporting OpenTelemetry traces");
            SdkTracerProviderUtil.setTracerConfigurator(tracerProvider, setExportEnabled(true));
        }
        return new TracingStatus(exportEnabled.get(), since.get());
    }

    @Override
    public TracingStatus disableExport()
    {
        if (exportEnabled.compareAndSet(true, false)) {
            since.set(Instant.now());
            log.info("Disabling exporting OpenTelemetry traces");
            SdkTracerProviderUtil.setTracerConfigurator(tracerProvider, setExportEnabled(false));
        }
        return new TracingStatus(exportEnabled.get(), since.get());
    }

    @Override
    public TracingStatus getStatus()
    {
        return new TracingStatus(exportEnabled.get(), since.get());
    }

    static ScopeConfigurator<TracerConfig> setExportEnabled(boolean enabled)
    {
        since.set(Instant.now());
        return _ -> enabled ? TracerConfig.enabled() : TracerConfig.disabled();
    }
}
