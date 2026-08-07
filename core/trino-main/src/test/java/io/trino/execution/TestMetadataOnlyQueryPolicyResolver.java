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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.RetryPolicy;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

class TestMetadataOnlyQueryPolicyResolver
{
    @Test
    void testNonTaskRetryPolicyPassThrough()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = policyResolverWithDefaults();
        Statement query = selectFrom("information_schema", "tables");

        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(NONE), query, Collections.emptyList()), NONE);
        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(QUERY), query, Collections.emptyList()), QUERY);
    }

    @Test
    void testTaskRetryPolicyEligibleQuery()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = policyResolverWithDefaults();

        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), selectFrom("hive", "my_schema", "my_table"), Collections.emptyList()), TASK);
    }

    @Test
    void testInformationSchemaQueryExcluded()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = policyResolverWithDefaults();

        // 2-part: schema.table where schema = information_schema
        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), selectFrom("information_schema", "tables"), internalSchemaTableHandles()), NONE);
        // 3-part: catalog.information_schema.table
        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), selectFrom("hive", "information_schema", "tables"), internalSchemaTableHandles()), NONE);
    }

    @Test
    void testSystemCatalogQueryExcluded()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = policyResolverWithDefaults();

        // 3-part: system.schema.table
        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), selectFrom("system", "runtime", "nodes"), systemTableHandles()), NONE);
    }

    @Test
    void testMixedTablesNotExcluded()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = policyResolverWithDefaults();

        // query referencing both a regular table and information_schema → not excluded
        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), selectFrom("hive", "my_schema", "my_table"), Collections.emptyList()), TASK);
    }

    @Test
    void testDescribeQueryTypeExcluded()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = new MetadataOnlyQueryPolicyResolver(
                new QueryManagerConfig()
                        .setRetryPolicyExcludedQueryTypes(ImmutableSet.of(QueryType.DESCRIBE))
                        .setRetryPolicyExcludeMetadataOnlyQueries(false));

        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), showTables(), Collections.emptyList()), NONE);
    }

    @Test
    void testExcludeMetadataOnlyQueriesDisabled()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = new MetadataOnlyQueryPolicyResolver(
                new QueryManagerConfig()
                        .setRetryPolicyExcludedQueryTypes(ImmutableSet.of())
                        .setRetryPolicyExcludeMetadataOnlyQueries(false));

        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), selectFrom("information_schema", "tables"), Collections.emptyList()), TASK);
    }

    @Test
    void testNonQueryStatementNotExcludedAsMetadataOnly()
    {
        MetadataOnlyQueryPolicyResolver policyResolver = policyResolverWithDefaults();

        // DEALLOCATE is not a Query node — metadata-only check does not apply
        assertEffectiveRetryPolicy(policyResolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(TASK), deallocate(), Collections.emptyList()), TASK);
    }

    private static void assertEffectiveRetryPolicy(Session result, RetryPolicy expected)
    {
        assertThat(getRetryPolicy(result)).isEqualTo(expected);
    }

    private static MetadataOnlyQueryPolicyResolver policyResolverWithDefaults()
    {
        return new MetadataOnlyQueryPolicyResolver(new QueryManagerConfig());
    }

    private static Session sessionWithRetryPolicy(RetryPolicy retryPolicy)
    {
        return testSessionBuilder()
                .setSystemProperty(RETRY_POLICY, retryPolicy.name())
                .build();
    }

    private static Query selectFrom(String schema, String table)
    {
        return queryWithTable(QualifiedName.of(schema, table));
    }

    private static Query selectFrom(String catalog, String schema, String table)
    {
        return queryWithTable(QualifiedName.of(catalog, schema, table));
    }

    private static Query queryWithTable(QualifiedName name)
    {
        return new Query(
                new NodeLocation(1, 1),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                new TableSubquery(new Query(
                        new NodeLocation(1, 1),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        new Table(new NodeLocation(1, 1), name),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static Statement showTables()
    {
        return new ShowTables(new NodeLocation(1, 1), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static Statement deallocate()
    {
        return new Deallocate(new NodeLocation(1, 1), new Identifier("foo"));
    }

    private static List<TableHandle> internalSchemaTableHandles()
    {
        CatalogHandle rootCatalogHandle = CatalogHandle.createRootCatalogHandle(
                new CatalogName("test"),
                new CatalogVersion("0"));
        CatalogHandle informationSchemaCatalogHandle = CatalogHandle.createInformationSchemaCatalogHandle(rootCatalogHandle);
        return List.of(new TableHandle(
                informationSchemaCatalogHandle,
                new TestingTableHandle(new SchemaTableName("information_schema", "tables")),
                TestingConnectorTransactionHandle.INSTANCE));
    }

    private static List<TableHandle> systemTableHandles()
    {
        CatalogHandle rootCatalogHandle = CatalogHandle.createRootCatalogHandle(
                new CatalogName("system"),
                new CatalogVersion("0"));
        CatalogHandle systemCatalogHandle = CatalogHandle.createSystemTablesCatalogHandle(rootCatalogHandle);
        return List.of(new TableHandle(
                systemCatalogHandle,
                new TestingTableHandle(new SchemaTableName("runtime", "nodes")),
                TestingConnectorTransactionHandle.INSTANCE));
    }
}
