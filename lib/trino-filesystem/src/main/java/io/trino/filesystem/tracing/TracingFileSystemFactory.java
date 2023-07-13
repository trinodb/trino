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
package io.trino.filesystem.tracing;

import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public final class TracingFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final Tracer tracer;
    private final TrinoFileSystemFactory delegate;

    public TracingFileSystemFactory(Tracer tracer, TrinoFileSystemFactory delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new TracingFileSystem(tracer, delegate.create(identity));
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session)
    {
        return new TracingFileSystem(tracer, delegate.create(session));
    }
}
