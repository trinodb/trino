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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.RetryPolicy;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableFunctionInvocation;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Unnest;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;

class TestMetadataOnlyQueryPolicyResolver
{
    private static final MetadataOnlyQueryPolicyResolver RESOLVER = new MetadataOnlyQueryPolicyResolver(new QueryManagerConfig());

    @Test
    void testRetryPolicyNoneStaysUnchanged()
    {
        assertThat(effectiveRetryPolicy(NONE, analysis(someQuery(), "hive.information_schema.tables"))).isEqualTo(NONE);
    }

    @Test
    void testRetryPolicyQueryStaysUnchanged()
    {
        // query retries still protect against network and connector failures, so there is nothing to gain by dropping them
        assertThat(effectiveRetryPolicy(QUERY, analysis(someQuery(), "hive.information_schema.tables"))).isEqualTo(QUERY);
    }

    @Test
    void testTaskRetryPolicyIsDowngradedToQuery()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "hive.information_schema.tables"))).isEqualTo(QUERY);
    }

    @Test
    void testTaskRetryPolicyIsDowngradedToNoneWhenQueryPolicyIsNotAllowed()
    {
        MetadataOnlyQueryPolicyResolver resolver = new MetadataOnlyQueryPolicyResolver(
                new QueryManagerConfig().setAllowedRetryPolicies(ImmutableSet.of(NONE, TASK)));

        assertThat(effectiveRetryPolicy(resolver, TASK, analysis(someQuery(), "hive.information_schema.tables"))).isEqualTo(NONE);
    }

    @Test
    void testInformationSchemaOfAnyCatalogIsMetadataOnly()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "hive.information_schema.tables"))).isEqualTo(QUERY);
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "system.information_schema.tables"))).isEqualTo(QUERY);
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "hive.information_schema.columns", "iceberg.information_schema.tables"))).isEqualTo(QUERY);
    }

    @Test
    void testSystemMetadataSchemasAreMetadataOnly()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "system.jdbc.tables"))).isEqualTo(QUERY);
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "system.metadata.table_comments"))).isEqualTo(QUERY);
    }

    @Test
    void testOtherSystemSchemasKeepRetryPolicy()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "system.runtime.queries"))).isEqualTo(TASK);
    }

    @Test
    void testConnectorSystemTableKeepsRetryPolicy()
    {
        // a table such as iceberg.my_schema."my_table$files" is served by the connector, not by the coordinator
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "iceberg.my_schema.my_table$files"))).isEqualTo(TASK);
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "iceberg.jdbc.my_table"))).isEqualTo(TASK);
    }

    @Test
    void testRegularTableKeepsRetryPolicy()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "hive.my_schema.my_table"))).isEqualTo(TASK);
    }

    @Test
    void testMixOfMetadataAndRegularTablesKeepsRetryPolicy()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery(), "hive.information_schema.tables", "hive.my_schema.my_table"))).isEqualTo(TASK);
    }

    @Test
    void testQueryWithoutTablesIsMetadataOnly()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(someQuery()))).isEqualTo(QUERY);
    }

    @Test
    void testUnnestKeepsRetryPolicy()
    {
        Analysis analysis = analysis(someQuery());
        analysis.setUnnest(new Unnest(ImmutableList.of(), false), new Analysis.UnnestAnalysis(ImmutableMap.of(), Optional.empty()));

        assertThat(effectiveRetryPolicy(TASK, analysis)).isEqualTo(TASK);
    }

    @Test
    void testTableFunctionKeepsRetryPolicy()
    {
        Analysis analysis = analysis(someQuery(), "hive.information_schema.tables");
        analysis.setTableFunctionAnalysis(
                new TableFunctionInvocation(new NodeLocation(1, 1), QualifiedName.of("some_function"), ImmutableList.of(), ImmutableList.of()),
                new Analysis.TableFunctionInvocationAnalysis(
                        TEST_CATALOG_HANDLE,
                        "some_function",
                        ImmutableMap.of(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        ImmutableList.of(),
                        0,
                        new ConnectorTableFunctionHandle() {},
                        TestingTransactionHandle.create()));

        assertThat(effectiveRetryPolicy(TASK, analysis)).isEqualTo(TASK);
    }

    @Test
    void testNonQueryStatementKeepsRetryPolicy()
    {
        assertThat(effectiveRetryPolicy(TASK, analysis(deallocate(), "hive.information_schema.tables"))).isEqualTo(TASK);
    }

    @Test
    void testExclusionCanBeDisabled()
    {
        MetadataOnlyQueryPolicyResolver resolver = new MetadataOnlyQueryPolicyResolver(
                new QueryManagerConfig().setRetryPolicyExcludeMetadataOnlyQueries(false));

        assertThat(effectiveRetryPolicy(resolver, TASK, analysis(someQuery(), "hive.information_schema.tables"))).isEqualTo(TASK);
    }

    private static RetryPolicy effectiveRetryPolicy(RetryPolicy retryPolicy, Analysis analysis)
    {
        return effectiveRetryPolicy(RESOLVER, retryPolicy, analysis);
    }

    private static RetryPolicy effectiveRetryPolicy(MetadataOnlyQueryPolicyResolver resolver, RetryPolicy retryPolicy, Analysis analysis)
    {
        return getRetryPolicy(resolver.getSessionWithEffectiveRetryPolicy(sessionWithRetryPolicy(retryPolicy), analysis));
    }

    private static Session sessionWithRetryPolicy(RetryPolicy retryPolicy)
    {
        return testSessionBuilder()
                .setSystemProperty(RETRY_POLICY, retryPolicy.name())
                .build();
    }

    private static Analysis analysis(Statement statement, String... tableNames)
    {
        Analysis analysis = new Analysis(statement, ImmutableMap.of(), OTHERS);
        stream(tableNames).forEach(name -> {
            String[] parts = name.split("\\.", 3);
            analysis.registerTable(
                    new Table(new NodeLocation(1, 1), QualifiedName.of(parts[0], parts[1], parts[2])),
                    Optional.empty(),
                    new QualifiedObjectName(parts[0], parts[1], parts[2]),
                    Optional.empty(),
                    "user",
                    Scope.create(),
                    Optional.empty());
        });
        return analysis;
    }

    private static Statement someQuery()
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
                        new Table(new NodeLocation(1, 1), QualifiedName.of("some_table")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static Statement deallocate()
    {
        return new Deallocate(new NodeLocation(1, 1), new Identifier("foo"));
    }
}
