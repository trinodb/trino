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
package io.trino.security;

import io.trino.SessionRepresentation;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.security.Identity;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public final class AccessControlUtil
{
    private AccessControlUtil() {}

    public static void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner, AccessControl accessControl)
    {
        if (identity.getUser().equals(queryOwner.getUser())) {
            return;
        }
        accessControl.checkCanViewQueryOwnedBy(identity, queryOwner);
    }

    public static List<BasicQueryInfo> filterQueries(Identity identity, List<BasicQueryInfo> queries, AccessControl accessControl)
    {
        Collection<Identity> owners = queries.stream()
                .map(BasicQueryInfo::getSession)
                .map(SessionRepresentation::toIdentity)
                .filter(owner -> !owner.getUser().equals(identity.getUser()))
                .map(FullIdentityEquality::new)
                .distinct()
                .map(FullIdentityEquality::getIdentity)
                .collect(toImmutableList());
        owners = accessControl.filterQueriesOwnedBy(identity, owners);

        Set<FullIdentityEquality> allowedOwners = owners.stream()
                .map(FullIdentityEquality::new)
                .collect(toImmutableSet());
        return queries.stream()
                .filter(queryInfo -> {
                    Identity queryIdentity = queryInfo.getSession().toIdentity();
                    return queryIdentity.getUser().equals(identity.getUser()) || allowedOwners.contains(new FullIdentityEquality(queryIdentity));
                })
                .collect(toImmutableList());
    }

    public static void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner, AccessControl accessControl)
    {
        if (identity.getUser().equals(queryOwner.getUser())) {
            return;
        }
        accessControl.checkCanKillQueryOwnedBy(identity, queryOwner);
    }

    private static class FullIdentityEquality
    {
        private final Identity identity;

        public FullIdentityEquality(Identity identity)
        {
            this.identity = identity;
        }

        public Identity getIdentity()
        {
            return identity;
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
            FullIdentityEquality that = (FullIdentityEquality) o;
            return Objects.equals(identity.getUser(), that.identity.getUser()) &&
                    Objects.equals(identity.getGroups(), that.identity.getGroups()) &&
                    Objects.equals(identity.getPrincipal(), that.identity.getPrincipal()) &&
                    Objects.equals(identity.getEnabledRoles(), that.identity.getEnabledRoles()) &&
                    Objects.equals(identity.getCatalogRoles(), that.identity.getCatalogRoles());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    identity.getUser(),
                    identity.getGroups(),
                    identity.getPrincipal(),
                    identity.getEnabledRoles(),
                    identity.getCatalogRoles());
        }
    }
}
