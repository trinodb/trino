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
package io.trino.metadata;

import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public record TableHandle(CatalogHandle catalogHandle, ConnectorTableHandle connectorHandle, ConnectorTransactionHandle transaction, ResolvingIdentity resolvingIdentity)
{
    public TableHandle
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorHandle, "connectorHandle is null");
        requireNonNull(transaction, "transaction is null");
        requireNonNull(resolvingIdentity, "resolvingIdentity is null");
    }

    public TableHandle withConnectorHandle(ConnectorTableHandle connectorHandle)
    {
        return new TableHandle(
                catalogHandle,
                connectorHandle,
                transaction,
                resolvingIdentity);
    }

    public boolean isResolvedAs(Identity identity)
    {
        return resolvingIdentity.equals(ResolvingIdentity.from(identity.toConnectorIdentity(catalogHandle.getCatalogName().toString())));
    }

    @Override
    public String toString()
    {
        return catalogHandle + ":" + connectorHandle;
    }

    /**
     * The identity the table was resolved as, for example the owner of a SECURITY DEFINER view.
     * Extra credentials are not included, the same way they are kept out of the session representation.
     * Only the session identity has extra credentials, so a table resolved with them is used with the session identity.
     */
    public record ResolvingIdentity(String user, Set<String> groups, Optional<String> principal, Set<String> enabledSystemRoles, Optional<SelectedRole> connectorRole, boolean hasExtraCredentials)
    {
        public ResolvingIdentity
        {
            requireNonNull(user, "user is null");
            groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
            requireNonNull(principal, "principal is null");
            enabledSystemRoles = ImmutableSet.copyOf(requireNonNull(enabledSystemRoles, "enabledSystemRoles is null"));
            requireNonNull(connectorRole, "connectorRole is null");
        }

        public static ResolvingIdentity from(ConnectorIdentity identity)
        {
            return new ResolvingIdentity(
                    identity.getUser(),
                    identity.getGroups(),
                    identity.getPrincipal().map(Principal::toString),
                    identity.getEnabledSystemRoles(),
                    identity.getConnectorRole(),
                    !identity.getExtraCredentials().isEmpty());
        }

        public ConnectorIdentity toConnectorIdentity()
        {
            checkExtraCredentialsNotRequired();
            return ConnectorIdentity.forUser(user)
                    .withGroups(groups)
                    .withPrincipal(principal.map(BasicPrincipal::new))
                    .withEnabledSystemRoles(enabledSystemRoles)
                    .withConnectorRole(connectorRole)
                    .build();
        }

        // The connector role applies only to the catalog of the table
        public Identity toIdentity(String catalogName)
        {
            checkExtraCredentialsNotRequired();
            Identity.Builder identity = Identity.forUser(user)
                    .withGroups(groups)
                    .withPrincipal(principal.map(BasicPrincipal::new))
                    .withEnabledRoles(enabledSystemRoles);
            connectorRole.ifPresent(role -> identity.withConnectorRole(catalogName, role));
            return identity.build();
        }

        private void checkExtraCredentialsNotRequired()
        {
            checkState(!hasExtraCredentials, "Extra credentials of the identity %s are not available for the table", user);
        }
    }
}
