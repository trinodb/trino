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
package io.trino.sql.planner;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvingIdentity;
import io.trino.metadata.TableHandle;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.testing.TestingHandles.TEST_RESOLVING_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

final class TestTableResolvingIdentities
{
    private static final ResolvingIdentity VIEW_OWNER = ResolvingIdentity.from(ConnectorIdentity.ofUser("view_owner"));

    @Test
    void testIdentitiesOfTheTablesOfAFragmentAreCollected()
    {
        assertThat(TableResolvingIdentities.collect(PLAN_FRAGMENT.getRoot()))
                .containsExactly(entry(TABLE_SCAN_NODE_ID, TEST_RESOLVING_IDENTITY));
    }

    @Test
    void testIdentitiesAreRestoredInADeserializedFragment()
    {
        PlanFragment fragment = withResolvingIdentity(PLAN_FRAGMENT, VIEW_OWNER);
        Map<PlanNodeId, ResolvingIdentity> identities = TableResolvingIdentities.collect(fragment.getRoot());

        PlanFragment restored = TableResolvingIdentities.restoreIn(deserialized(fragment), identities);

        assertThat(tableOf(restored).requireResolvingIdentity()).isEqualTo(VIEW_OWNER);
    }

    @Test
    void testRestoringFailsWhenAnIdentityIsMissing()
    {
        PlanFragment fragment = deserialized(PLAN_FRAGMENT);

        assertThatThrownBy(() -> TableResolvingIdentities.restoreIn(fragment, ImmutableMap.of()))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("No identity for the table of the plan node " + TABLE_SCAN_NODE_ID);
    }

    @Test
    void testFragmentWhichWasNotSerializedKeepsItsIdentities()
    {
        PlanFragment restored = TableResolvingIdentities.restoreIn(withResolvingIdentity(PLAN_FRAGMENT, VIEW_OWNER), ImmutableMap.of());

        assertThat(tableOf(restored).requireResolvingIdentity()).isEqualTo(VIEW_OWNER);
    }

    private static PlanFragment withResolvingIdentity(PlanFragment fragment, ResolvingIdentity identity)
    {
        return withTable(fragment, tableOf(fragment).withResolvingIdentity(identity));
    }

    /**
     * A fragment as a worker receives it: the serialized form of a table handle does not carry its identity.
     */
    private static PlanFragment deserialized(PlanFragment fragment)
    {
        TableHandle table = tableOf(fragment);
        return withTable(fragment, new TableHandle(table.catalogHandle(), table.connectorHandle(), table.transaction(), Optional.empty()));
    }

    private static PlanFragment withTable(PlanFragment fragment, TableHandle table)
    {
        return fragment.withRoot(((TableScanNode) fragment.getRoot()).withTableHandle(table));
    }

    private static TableHandle tableOf(PlanFragment fragment)
    {
        return ((TableScanNode) fragment.getRoot()).getTable();
    }
}
