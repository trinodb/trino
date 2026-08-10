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
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.RetryPolicy;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Query;

import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.operator.RetryPolicy.TASK;
import static java.util.Objects.requireNonNull;

public class MetadataOnlyQueryPolicyResolver
{
    // Schemas of the global system catalog that only expose metadata
    private static final Set<String> SYSTEM_METADATA_SCHEMAS = ImmutableSet.of("jdbc", "metadata");

    private final Optional<RetryPolicy> metadataOnlyQueryRetryPolicy;

    @Inject
    public MetadataOnlyQueryPolicyResolver(QueryManagerConfig config)
    {
        requireNonNull(config, "config is null");
        // Query-level retries are preferred over no retries at all, because they still make the query resilient
        // against network and connector failures
        this.metadataOnlyQueryRetryPolicy = config.isRetryPolicyExcludeMetadataOnlyQueries()
                ? Optional.of(config.getAllowedRetryPolicies().contains(QUERY) ? QUERY : NONE)
                : Optional.empty();
    }

    public Session getSessionWithEffectiveRetryPolicy(Session session, Analysis analysis)
    {
        if (metadataOnlyQueryRetryPolicy.isEmpty()) {
            return session;
        }
        // Metadata queries are served by the coordinator, so task-level retries only add the overhead of exchange
        // spooling without ever retrying a task
        if (getRetryPolicy(session) == TASK && isMetadataOnlyQuery(analysis)) {
            return session.withRetryPolicy(metadataOnlyQueryRetryPolicy.get());
        }
        return session;
    }

    private static boolean isMetadataOnlyQuery(Analysis analysis)
    {
        // Restricted to read-only queries so that writes are never downgraded. SHOW and DESCRIBE statements are
        // rewritten before the analysis, so they reach this point as queries.
        if (!(analysis.getStatement() instanceof Query)) {
            return false;
        }
        // Table functions and UNNEST generate rows regardless of the tables the query reads
        if (analysis.hasTableFunctions() || analysis.hasUnnest()) {
            return false;
        }
        return analysis.getTableNames().stream().allMatch(MetadataOnlyQueryPolicyResolver::isMetadataTable);
    }

    private static boolean isMetadataTable(QualifiedObjectName table)
    {
        if (table.schemaName().equals(INFORMATION_SCHEMA)) {
            return true;
        }
        return table.catalogName().equals(GlobalSystemConnector.NAME) && SYSTEM_METADATA_SCHEMAS.contains(table.schemaName());
    }
}
