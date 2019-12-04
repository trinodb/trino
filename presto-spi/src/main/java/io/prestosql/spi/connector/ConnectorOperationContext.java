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
package io.prestosql.spi.connector;

import io.prestosql.spi.tracer.ConnectorTracer;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnectorOperationContext
{
    private final Optional<ConnectorTracer> tracer;

    private ConnectorOperationContext(Optional<ConnectorTracer> tracer)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    public static ConnectorOperationContext createNoOpConnectorOperationContext()
    {
        return (new ConnectorOperationContext.Builder()).withConnectorTracer(Optional.empty()).build();
    }

    public Optional<ConnectorTracer> getConnectorTracer()
    {
        return tracer;
    }

    public static class Builder
    {
        private Optional<ConnectorTracer> tracer;

        public Builder withConnectorTracer(Optional<ConnectorTracer> tracer)
        {
            this.tracer = requireNonNull(tracer, "tracer is null");
            return this;
        }

        public ConnectorOperationContext build()
        {
            return new ConnectorOperationContext(tracer);
        }
    }
}
