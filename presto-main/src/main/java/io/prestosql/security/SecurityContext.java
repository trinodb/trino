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
package io.prestosql.security;

import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.transaction.TransactionId;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SecurityContext
{
    public static SecurityContext of(Session session)
    {
        requireNonNull(session, "session is null");
        return new SecurityContext(session.getRequiredTransactionId(), session.getIdentity());
    }

    private final TransactionId transactionId;
    private final Identity identity;

    public SecurityContext(TransactionId transactionId, Identity identity)
    {
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.identity = requireNonNull(identity, "identity is null");
    }

    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public SystemSecurityContext toSystemSecurityContext()
    {
        return new SystemSecurityContext(identity);
    }

    @Override
    public boolean equals(Object o)
    {
        // this is needed by io.prestosql.sql.analyzer.Analysis.AccessControlInfo
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SecurityContext that = (SecurityContext) o;
        return Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(identity, that.identity);
    }

    @Override
    public int hashCode()
    {
        // this is needed by io.prestosql.sql.analyzer.Analysis.AccessControlInfo
        return Objects.hash(transactionId, identity);
    }
}
