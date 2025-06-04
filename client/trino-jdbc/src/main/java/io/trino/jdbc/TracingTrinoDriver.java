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
package io.trino.jdbc;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.okhttp.v3_0.OkHttpTelemetry;
import okhttp3.Call;
import okhttp3.OkHttpClient;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TracingTrinoDriver
        extends NonRegisteringTrinoDriver
{
    private final OpenTelemetry openTelemetry;

    public TracingTrinoDriver(OpenTelemetry openTelemetry)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    public TracingTrinoDriver()
    {
        this(GlobalOpenTelemetry.get());
    }

    @Override
    protected Call.Factory wrapClient(OkHttpClient client)
    {
        return OkHttpTelemetry
                .builder(Optional.ofNullable(openTelemetry).orElse(GlobalOpenTelemetry.get()))
                .build()
                .newCallFactory(client);
    }
}
