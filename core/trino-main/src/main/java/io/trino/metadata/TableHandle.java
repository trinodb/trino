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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.Identity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A table, as resolved by {@link Metadata}, together with the identity it was resolved as.
 * <p>
 * The identity is not serialized, so that query information, the stage plans it carries to the events of a listener,
 * and {@code EXPLAIN} output do not expose the identity of a SECURITY DEFINER view's owner. A worker receives the
 * identities of the tables a plan fragment reads next to the fragment and restores them before planning it. The other
 * handles of a fragment, such as the targets of writes, are used with the session identity and have no identity on
 * a worker.
 */
public record TableHandle(
        CatalogHandle catalogHandle,
        ConnectorTableHandle connectorHandle,
        ConnectorTransactionHandle transaction,
        Optional<ResolvingIdentity> resolvingIdentity)
{
    public TableHandle
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorHandle, "connectorHandle is null");
        requireNonNull(transaction, "transaction is null");
        requireNonNull(resolvingIdentity, "resolvingIdentity is null");
    }

    public TableHandle(CatalogHandle catalogHandle, ConnectorTableHandle connectorHandle, ConnectorTransactionHandle transaction, ResolvingIdentity resolvingIdentity)
    {
        this(catalogHandle, connectorHandle, transaction, Optional.of(resolvingIdentity));
    }

    @DoNotCall // For JSON deserialization only
    @JsonCreator
    public static TableHandle fromJson(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("connectorHandle") ConnectorTableHandle connectorHandle,
            @JsonProperty("transaction") ConnectorTransactionHandle transaction)
    {
        return new TableHandle(catalogHandle, connectorHandle, transaction, Optional.empty());
    }

    /**
     * The identity the table was resolved as. Empty in a deserialized handle whose identity has not been restored.
     */
    @JsonIgnore
    @Override
    public Optional<ResolvingIdentity> resolvingIdentity()
    {
        return resolvingIdentity;
    }

    public TableHandle withConnectorHandle(ConnectorTableHandle connectorHandle)
    {
        return new TableHandle(
                catalogHandle,
                connectorHandle,
                transaction,
                resolvingIdentity);
    }

    public TableHandle withResolvingIdentity(ResolvingIdentity resolvingIdentity)
    {
        return new TableHandle(
                catalogHandle,
                connectorHandle,
                transaction,
                resolvingIdentity);
    }

    /**
     * Whether the table was resolved as {@code identity}, so that it can be used with a session of that identity,
     * including its extra credentials.
     */
    public boolean isResolvedAs(Identity identity)
    {
        return requireResolvingIdentity().equals(ResolvingIdentity.from(identity.toConnectorIdentity(catalogHandle.getCatalogName().toString())));
    }

    public ResolvingIdentity requireResolvingIdentity()
    {
        return resolvingIdentity.orElseThrow(() -> new IllegalStateException("The identity of the table " + this + " has not been restored"));
    }

    @Override
    public String toString()
    {
        return catalogHandle + ":" + connectorHandle;
    }
}
