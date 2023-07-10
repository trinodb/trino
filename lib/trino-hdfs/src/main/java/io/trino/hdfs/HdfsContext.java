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
package io.trino.hdfs;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HdfsContext
{
    private final ConnectorIdentity identity;

    public HdfsContext(ConnectorIdentity identity)
    {
        this.identity = requireNonNull(identity, "identity is null");
    }

    public HdfsContext(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
    }

    public ConnectorIdentity getIdentity()
    {
        return identity;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("user", identity)
                .toString();
    }
}
