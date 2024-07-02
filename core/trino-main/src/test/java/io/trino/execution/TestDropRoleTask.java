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

import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

public class TestDropRoleTask
        extends BaseRoleTaskTest
{
    @Test
    void testDropUnknownRole()
    {
        assertTrinoExceptionThrownBy(() -> executeDropRole("DROP ROLE nonexistentrole1234"))
                .hasErrorCode(ROLE_NOT_FOUND)
                .hasMessage("line 1:1: Role 'nonexistentrole1234' does not exist");
        executeDropRole("DROP ROLE IF EXISTS nonexistantrole1234"); // Shouldn't throw.
    }

    private QueryStateMachine executeDropRole(String statement)
    {
        return execute(statement, new DropRoleTask(metadata, accessControl));
    }
}
