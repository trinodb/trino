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
package io.trino.plugin.deltalake;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DefaultDeltaLakeFileSystemFactory
        implements DeltaLakeFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeTableCredentialsProvider tableCredentialsProvider;

    @Inject
    public DefaultDeltaLakeFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, DeltaLakeTableCredentialsProvider tableCredentialsProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableCredentialsProvider = requireNonNull(tableCredentialsProvider, "tableCredentialsProvider is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        ConnectorIdentity identity = session.getIdentity();
        if (tableCredentials.isPresent()) {
            // Do not include original credentials as they should not be used in vended mode
            ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                    .withGroups(identity.getGroups())
                    .withPrincipal(identity.getPrincipal())
                    .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                    .withConnectorRole(identity.getConnectorRole())
                    .withExtraCredentials(tableCredentials.get().fileSystemCredentials().asExtraCredentials())
                    .build();
            return fileSystemFactory.create(identityWithExtraCredentials);
        }

        return fileSystemFactory.create(identity);
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, String tableLocation)
    {
        Optional<DeltaLakeTableCredentials> tableCredentials = tableCredentialsProvider.getTableCredentials(VendedCredentialsHandle.empty(tableLocation));
        return create(session, tableCredentials);
    }
}
