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
package io.prestosql.security;

import com.google.common.collect.ImmutableSet;
import io.prestosql.SessionRepresentation;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.security.Identity;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public final class AccessControlUtil
{
    private AccessControlUtil() {}

    public static void checkCanViewQueryOwnedBy(Identity identity, String queryOwner, AccessControl accessControl)
    {
        if (identity.getUser().equals(queryOwner)) {
            return;
        }
        accessControl.checkCanViewQueryOwnedBy(identity, queryOwner);
    }

    public static List<BasicQueryInfo> filterQueries(Identity identity, List<BasicQueryInfo> queries, AccessControl accessControl)
    {
        String currentUser = identity.getUser();
        Set<String> owners = queries.stream()
                .map(BasicQueryInfo::getSession)
                .map(SessionRepresentation::getUser)
                .filter(owner -> !owner.equals(currentUser))
                .collect(toImmutableSet());
        owners = accessControl.filterQueriesOwnedBy(identity, owners);

        Set<String> allowedOwners = ImmutableSet.<String>builder()
                .add(currentUser)
                .addAll(owners)
                .build();
        return queries.stream()
                .filter(queryInfo -> allowedOwners.contains(queryInfo.getSession().getUser()))
                .collect(toImmutableList());
    }

    public static void checkCanKillQueryOwnedBy(Identity identity, String queryOwner, AccessControl accessControl)
    {
        if (identity.getUser().equals(queryOwner)) {
            return;
        }
        accessControl.checkCanKillQueryOwnedBy(identity, queryOwner);
    }
}
