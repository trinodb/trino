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
package io.prestosql.connector;

import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.prestosql.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;

public class MutableGrants<T>
        implements Grants<T>
{
    private final Map<Grant, Boolean> grants = new HashMap<>();

    @Override
    public void grant(PrestoPrincipal principal, T objectName, Set<Privilege> privileges, boolean grantOption)
    {
        privileges.forEach(privilege -> {
            Grant grant = new Grant(principal, objectName, privilege);
            grants.put(grant, grantOption || grants.getOrDefault(grant, false));
        });
    }

    @Override
    public void revoke(PrestoPrincipal principal, T objectName, Set<Privilege> privileges, boolean grantOption)
    {
        privileges.forEach(privilege -> grants.remove(new Grant(principal, objectName, privilege)));
    }

    @Override
    public boolean isAllowed(String user, T objectName, Privilege privilege)
    {
        return grants.containsKey(new Grant(new PrestoPrincipal(USER, user), objectName, privilege));
    }

    @Override
    public boolean canGrant(String user, T objectName, Privilege privilege)
    {
        return grants.getOrDefault(new Grant(new PrestoPrincipal(USER, user), objectName, privilege), false);
    }

    class Grant
    {
        private final PrestoPrincipal principal;
        private final T objectName;
        private final Privilege privilege;

        Grant(PrestoPrincipal principal, T objectName, Privilege privilege)
        {
            this.principal = requireNonNull(principal, "principal is null");
            this.objectName = requireNonNull(objectName, "objectName is null");
            this.privilege = requireNonNull(privilege, "privilege is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Grant grant = (Grant) o;
            return principal.equals(grant.principal) &&
                    objectName.equals(grant.objectName) &&
                    privilege == grant.privilege;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(principal, objectName, privilege);
        }
    }
}
