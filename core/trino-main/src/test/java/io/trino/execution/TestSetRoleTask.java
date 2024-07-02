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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.SelectedRole;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSetRoleTask
        extends BaseRoleTaskTest
{
    @Test
    public void testSetRole()
    {
        assertSetRole("SET ROLE ALL IN " + CATALOG_NAME, ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.ALL, Optional.empty())));
        assertSetRole("SET ROLE NONE IN " + CATALOG_NAME, ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.NONE, Optional.empty())));
        assertSetRole("SET ROLE " + ROLE_NAME + " IN " + CATALOG_NAME, ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.ROLE, Optional.of(ROLE_NAME))));

        assertSetRole("SET ROLE ALL", ImmutableMap.of("system", new SelectedRole(SelectedRole.Type.ALL, Optional.empty())));
        assertSetRole("SET ROLE NONE", ImmutableMap.of("system", new SelectedRole(SelectedRole.Type.NONE, Optional.empty())));
    }

    @Test
    public void testSetRoleInvalidRole()
    {
        assertTrinoExceptionThrownBy(() -> executeSetRole("SET ROLE unknown IN " + CATALOG_NAME))
                .hasErrorCode(ROLE_NOT_FOUND)
                .hasMessage("line 1:1: Role 'unknown' does not exist");
    }

    @Test
    public void testSetRoleInvalidCatalog()
    {
        assertTrinoExceptionThrownBy(() -> executeSetRole("SET ROLE foo IN invalid"))
                .hasErrorCode(CATALOG_NOT_FOUND)
                .hasMessage("line 1:1: Catalog 'invalid' not found");
    }

    @Test
    public void testSetCatalogRoleInCatalogWithSystemSecurity()
    {
        assertTrinoExceptionThrownBy(() -> executeSetRole("SET ROLE foo IN " + SYSTEM_ROLE_CATALOG_NAME))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Catalog '" + SYSTEM_ROLE_CATALOG_NAME + "' does not support role management");
    }

    private void assertSetRole(String statement, Map<String, SelectedRole> expected)
    {
        QueryStateMachine stateMachine = executeSetRole(statement);
        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertThat(queryInfo.getSetRoles()).isEqualTo(expected);
    }

    private QueryStateMachine executeSetRole(String statement)
    {
        return execute(statement, new SetRoleTask(metadata, accessControl));
    }
}
