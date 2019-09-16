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
package io.prestosql;

import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class FullConnectorSecurityContext
        implements ConnectorSecurityContext
{
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorIdentity identity;

    public FullConnectorSecurityContext(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.identity = requireNonNull(identity, "identity is null");
    }

    @Override
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return identity;
    }
}
