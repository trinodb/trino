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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SwitchingFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final Optional<TrinoFileSystemFactory> hdfsFactory;
    private final Map<String, TrinoFileSystemFactory> factories;

    public SwitchingFileSystemFactory(Optional<TrinoFileSystemFactory> hdfsFactory, Map<String, TrinoFileSystemFactory> factories)
    {
        this.hdfsFactory = requireNonNull(hdfsFactory, "hdfsFactory is null");
        this.factories = ImmutableMap.copyOf(requireNonNull(factories, "factories is null"));
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session)
    {
        return new SwitchingFileSystem(Optional.of(session), Optional.empty(), hdfsFactory, factories);
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new SwitchingFileSystem(Optional.empty(), Optional.of(identity), hdfsFactory, factories);
    }
}
