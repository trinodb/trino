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
package io.trino.filesystem;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

public interface TrinoFileSystemFactory
{
    TrinoFileSystem create(ConnectorIdentity identity);

    default TrinoFileSystem create(ConnectorSession session)
    {
        return create(session.getIdentity());
    }

    /**
     * Creates a file system for the given session with explicit caching control.
     * The default implementation ignores the cachingEnabled parameter and delegates to {@link #create(ConnectorSession)}.
     * Implementations that support caching control should override this method.
     */
    default TrinoFileSystem create(ConnectorSession session, boolean cachingEnabled)
    {
        return create(session);
    }

    /**
     * Creates a file system for the given identity with explicit caching control.
     * The default implementation ignores the cachingEnabled parameter and delegates to {@link #create(ConnectorIdentity)}.
     * Implementations that support caching control should override this method.
     */
    default TrinoFileSystem create(ConnectorIdentity identity, boolean cachingEnabled)
    {
        return create(identity);
    }
}
