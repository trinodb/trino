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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DefaultIcebergFileSystemFactory
        implements IcebergFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public DefaultIcebergFileSystemFactory(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        return fileSystemFactory.create(enrichIdentityWithObjectTags(identity, fileIoProperties));
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, IcebergTableCredentials tableCredentials)
    {
        return create(identity, tableCredentials.fileIoProperties());
    }

    private static ConnectorIdentity enrichIdentityWithObjectTags(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        String objectTags = fileIoProperties.get("s3.object-tags");
        if (objectTags == null) {
            return identity;
        }
        return ConnectorIdentity.forUser(identity.getUser())
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                .withConnectorRole(identity.getConnectorRole())
                .withExtraCredentials(ImmutableMap.<String, String>builder()
                        .putAll(identity.getExtraCredentials())
                        .put("internal$s3_object_tags", objectTags)
                        .buildOrThrow())
                .build();
    }
}
