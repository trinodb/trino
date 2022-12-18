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

import static com.google.common.collect.ImmutableList.toImmutableList;

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
                .distinct()
                .collect(toImmutableList());
        Collection<Identity> allowedOwners = accessControl.filterQueriesOwnedBy(identity, owners);

        return queries.stream()
                .filter(queryInfo -> {
                    Identity queryIdentity = queryInfo.getSession().toIdentity();
                    return queryIdentity.getUser().equals(identity.getUser()) || allowedOwners.contains(queryIdentity);
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
}
