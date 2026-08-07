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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.metadata.TableHandle;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.util.StatementUtils.getQueryType;
import static java.util.Objects.requireNonNull;

public class MetadataOnlyQueryPolicyResolver
{
    private final Set<QueryType> excludedQueryTypes;
    private final boolean excludeMetadataOnlyQueries;

    @Inject
    public MetadataOnlyQueryPolicyResolver(QueryManagerConfig config)
    {
        requireNonNull(config, "config is null");
        this.excludedQueryTypes = ImmutableSet.copyOf(config.getRetryPolicyExcludedQueryTypes());
        this.excludeMetadataOnlyQueries = config.isRetryPolicyExcludeMetadataOnlyQueries();
    }

    public Session getSessionWithEffectiveRetryPolicy(Session session, Statement statement, Collection<TableHandle> tables)
    {
        if (getRetryPolicy(session) == TASK && isExcludedFromFaultTolerantExecution(statement, tables)) {
            return session.withRetryPolicy(NONE);
        }
        return session;
    }

    private boolean isExcludedFromFaultTolerantExecution(Statement statement, Collection<TableHandle> tables)
    {
        Optional<QueryType> queryType = getQueryType(statement);
        if (queryType.isPresent() && excludedQueryTypes.contains(queryType.get())) {
            return true;
        }
        return excludeMetadataOnlyQueries && isMetadataOnlyQuery(statement, tables);
    }

    private static boolean isMetadataOnlyQuery(Statement statement, Collection<TableHandle> tables)
    {
        if (!(statement instanceof Query)) {
            return false;
        }
        return !tables.isEmpty() && tables.stream()
                .map(TableHandle::catalogHandle)
                .allMatch(catalogHandle -> catalogHandle.getType().isInternal());
    }
}
