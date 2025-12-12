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
package io.trino.server;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.airlift.tracing.TracingModule;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;

import static java.util.Objects.requireNonNull;

/**
 * Conditionally uses GlobalOpenTelemetry if available (e.g., from Java agent),
 * otherwise falls back to creating a dedicated OpenTelemetry instance via TracingModule.
 * <p>
 * This allows seamless integration with OpenTelemetry Java agent while maintaining
 * backwards compatibility with standalone deployments.
 */
public class ConditionalTracingModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(ConditionalTracingModule.class);

    private final String serviceName;
    private final String serviceVersion;

    public ConditionalTracingModule(String serviceName, String serviceVersion)
    {
        this.serviceName = requireNonNull(serviceName, "serviceName is null");
        this.serviceVersion = requireNonNull(serviceVersion, "serviceVersion is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        // We need to check the config, but TracingConfig isn't bound yet in airlift's TracingModule
        // So we detect GlobalOpenTelemetry directly and provide an override mechanism
        boolean useGlobal = shouldUseGlobalOpenTelemetry();

        if (useGlobal) {
            log.info("Using GlobalOpenTelemetry (Java agent or external initialization detected)");
            install(new GlobalOpenTelemetryModule(serviceName, serviceVersion));
        }
        else {
            log.info("Using dedicated TracingModule");
            install(new TracingModule(serviceName, serviceVersion));
        }
    }

    private boolean shouldUseGlobalOpenTelemetry()
    {
        // Check for explicit system property override
        String useGlobalProperty = System.getProperty("tracing.use-global");
        if (useGlobalProperty != null) {
            boolean useGlobal = Boolean.parseBoolean(useGlobalProperty);
            log.info("Explicit system property: -Dtracing.use-global=%s", useGlobal);
            return useGlobal;
        }

        // Check for environment variable override
        String useGlobalEnv = System.getenv("TRACING_USE_GLOBAL");
        if (useGlobalEnv != null) {
            boolean useGlobal = Boolean.parseBoolean(useGlobalEnv);
            log.info("Explicit environment variable: TRACING_USE_GLOBAL=%s", useGlobal);
            return useGlobal;
        }

        // Auto-detect: Check if GlobalOpenTelemetry has been initialized
        // (typically by OpenTelemetry Java agent)
        try {
            OpenTelemetry global = GlobalOpenTelemetry.get();
            boolean isNoop = global.equals(OpenTelemetry.noop());

            if (!isNoop) {
                log.info("GlobalOpenTelemetry detected (likely from Java agent)");
                return true;
            }

            log.debug("GlobalOpenTelemetry is noop, will use dedicated instance");
            return false;
        }
        catch (Exception e) {
            log.debug(e, "Error checking GlobalOpenTelemetry, will use dedicated instance");
            return false;
        }
    }
}
