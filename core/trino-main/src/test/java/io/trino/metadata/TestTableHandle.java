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

import io.airlift.json.JsonMapperProvider;
import io.trino.metadata.TableHandle.ResolvingIdentity;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.SelectedRole;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTransactionHandle.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTableHandle
{
    private static final ResolvingIdentity OWNER = ResolvingIdentity.from(ConnectorIdentity.ofUser("view_owner"));
    private static final ResolvingIdentity INVOKER = ResolvingIdentity.from(ConnectorIdentity.ofUser("invoker"));

    public record TestingTableHandle(String id)
            implements ConnectorTableHandle {}

    private static final ConnectorTransactionHandle TRANSACTION = create();

    private static TableHandle tableHandle(ResolvingIdentity resolvingIdentity)
    {
        return new TableHandle(TEST_CATALOG_HANDLE, new TestingTableHandle("t"), TRANSACTION, resolvingIdentity);
    }

    @Test
    public void testResolvingIdentitySurvivesConnectorHandleRewrites()
    {
        TableHandle handle = tableHandle(OWNER);
        // a pushdown replaces the connector handle, but the table is still the one the owner resolved
        TableHandle rewritten = handle.withConnectorHandle(new TestingTableHandle("t2"));
        assertThat(rewritten.resolvingIdentity()).isEqualTo(OWNER);
        assertThat(rewritten.catalogHandle()).isEqualTo(handle.catalogHandle());
        assertThat(rewritten.transaction()).isEqualTo(handle.transaction());
    }

    @Test
    public void testTablesResolvedAsDifferentIdentitiesAreNotEqual()
    {
        // per-identity caches (e.g. table statistics) stay separate
        assertThat(tableHandle(OWNER)).isNotEqualTo(tableHandle(INVOKER));
        assertThat(tableHandle(OWNER)).isEqualTo(tableHandle(OWNER));
    }

    @Test
    public void testDifferentAuthorizationAttributesAreNotEqual()
    {
        List<ConnectorIdentity> identities = List.of(
                ConnectorIdentity.ofUser("owner"),
                ConnectorIdentity.ofUser("other_owner"),
                ConnectorIdentity.forUser("owner").withGroups(Set.of("group")).build(),
                ConnectorIdentity.forUser("owner").withPrincipal(new BasicPrincipal("principal")).build(),
                ConnectorIdentity.forUser("owner").withEnabledSystemRoles(Set.of("role")).build(),
                ConnectorIdentity.forUser("owner").withConnectorRole(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role"))).build());
        for (ConnectorIdentity left : identities) {
            for (ConnectorIdentity right : identities) {
                TableHandle first = tableHandle(ResolvingIdentity.from(left));
                TableHandle second = tableHandle(ResolvingIdentity.from(right));
                assertThat(first.equals(second)).isEqualTo(left == right);
            }
        }
    }

    @Test
    public void testResolvingIdentityRecordsOnlyPresenceOfExtraCredentials()
    {
        ConnectorIdentity withCredentials = ConnectorIdentity.forUser("owner")
                .withGroups(Set.of("b", "a"))
                .withExtraCredentials(Map.of("credential", "value"))
                .build();
        ResolvingIdentity resolvingIdentity = ResolvingIdentity.from(withCredentials);

        assertThat(resolvingIdentity.hasExtraCredentials()).isTrue();
        // a table resolved without credentials does not share scans or statistics with one resolved with them
        assertThat(resolvingIdentity).isNotEqualTo(ResolvingIdentity.from(ConnectorIdentity.forUser("owner").withGroups(Set.of("a", "b")).build()));
        // the credentials themselves are not carried, so this identity can only be used as the session identity
        assertThatThrownBy(resolvingIdentity::toConnectorIdentity)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Extra credentials of the identity owner are not available for the table");
    }

    @Test
    public void testWorkerSerializationCarriesResolvingIdentity()
            throws Exception
    {
        HandleResolver resolver = new HandleResolver();
        var mapper = new JsonMapperProvider()
                .withModules(Set.of(HandleJsonModule.tableHandleModule(resolver), HandleJsonModule.transactionHandleModule(resolver)))
                .get();
        TableHandle table = tableHandle(ResolvingIdentity.from(ConnectorIdentity.forUser("view_owner")
                .withGroups(Set.of("view_owners"))
                .withPrincipal(new BasicPrincipal("owner_principal"))
                .withConnectorRole(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("owner_role")))
                .withExtraCredentials(Map.of("private_key", "private_value"))
                .build()));
        String json = mapper.writeValueAsString(table);

        assertThat(json).contains("view_owner", "view_owners", "owner_principal", "owner_role");
        assertThat(json).doesNotContain("private_key", "private_value");
        assertThat(mapper.readValue(json, TableHandle.class)).isEqualTo(table);
    }
}
