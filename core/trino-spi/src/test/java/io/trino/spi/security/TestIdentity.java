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
package io.trino.spi.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIdentity
{
    private static final Identity TEST_IDENTITY = Identity.forUser("user")
            // part of identity:
            .withPrincipal(new BasicPrincipal("principal"))
            .withGroups(ImmutableSet.of("group1", "group2"))
            .withEnabledRoles(ImmutableSet.of("role1", "role2"))
            .withConnectorRoles(ImmutableMap.of(
                    "connector1", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("connector1role")),
                    "connector2", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("connector2role"))))
            // not part of identity:
            .withExtraCredentials(ImmutableMap.of("extra1", "credential1"))
            .build();

    @Test
    public void testEquals()
    {
        Identity otherIdentity = Identity.from(TEST_IDENTITY)
                // not part of identity:
                .withExtraCredentials(ImmutableMap.of("extra2", "credential2"))
                .build();

        assertThat(otherIdentity)
                .isEqualTo(TEST_IDENTITY);
    }

    @Test(dataProvider = "notEqualProvider")
    public void testNotEquals(Identity otherIdentity)
    {
        assertThat(otherIdentity)
                .isNotEqualTo(TEST_IDENTITY);
    }

    @DataProvider
    public static Object[][] notEqualProvider()
    {
        return new Object[][]
                {
                        {Identity.from(TEST_IDENTITY).withPrincipal(new BasicPrincipal("other principal")).build()},
                        {Identity.from(TEST_IDENTITY).withGroups(ImmutableSet.of("group2", "group3")).build()},
                        {Identity.from(TEST_IDENTITY).withEnabledRoles(ImmutableSet.of("role2", "role3")).build()},
                        {Identity.from(TEST_IDENTITY).withConnectorRoles(ImmutableMap.of(
                                "connector2", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("connector2role")),
                                "connector3", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("connector3role"))))
                                .build()},
                };
    }

    @Test
    public void testHashCode()
    {
        Identity otherIdentity = Identity.from(TEST_IDENTITY)
                // not part of identity:
                .withExtraCredentials(ImmutableMap.of("extra2", "credential2"))
                .build();

        assertThat(otherIdentity.hashCode())
                .isEqualTo(TEST_IDENTITY.hashCode());
    }
}
