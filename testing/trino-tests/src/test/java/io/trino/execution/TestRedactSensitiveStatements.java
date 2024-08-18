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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.EventsCollector.QueryEvents;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.plugin.jdbc.JdbcPlugin;
import io.trino.plugin.jdbc.TestingH2JdbcModule;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.jdbc.TestingH2JdbcModule.createH2ConnectionUrl;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRedactSensitiveStatements
        extends AbstractTestQueryFramework
{
    private final EventsCollector generatedEvents = new EventsCollector();
    private EventsAwaitingQueries queries;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        queryRunner.installPlugin(new JdbcPlugin("jdbc", new TestingH2JdbcModule()));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);

        return queryRunner;
    }

    @Test
    void testSyntacticallyInvalidStatementThatMayBeSensitive()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        QueryEvents queryEvents = runQueryAndWaitForEvents("""
                CREATE CCATALOG %s USING jdbc
                WITH (
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )""".formatted(catalog), ".*mismatched input 'CCATALOG'.*");

        assertRedactedQuery("""
                CREATE CCATALOG %s USING jdbc
                WITH (
                   "connection-user" = '***',
                   "connection-password" = '***'
                )""".formatted(catalog), queryEvents);
    }

    @Test
    void testSyntacticallyInvalidStatementThatShouldNotBeSensitive()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECTT * FROM nation WHERE name = '\"password\" = ''1234'''", ".*mismatched input 'SELECTT'.*");

        assertRedactedQuery("SELECTT * FROM nation WHERE name = '\"password\" = ''1234'''", queryEvents);
    }

    @Test
    void testStatementThatShouldNotBeSensitive()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT * FROM nation WHERE name = '\"password\" = ''1234'''");

        assertRedactedQuery("SELECT * FROM nation WHERE name = '\"password\" = ''1234'''", queryEvents);
    }

    @Test
    void testUnsupportedStatement()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        QueryEvents queryEvents = runQueryAndWaitForEvents("""
                EXPLAIN ANALYZE CREATE CATALOG %s USING jdbc
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )""".formatted(catalog), "EXPLAIN ANALYZE doesn't support statement type: CreateCatalog");

        assertRedactedQuery("""
                EXPLAIN ANALYZE CREATE CATALOG %s USING jdbc
                WITH (
                   "user" = '***',
                   "password" = '***'
                )""".formatted(catalog), queryEvents);
    }

    @Test
    void testCreateCatalog()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents("""
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )""".formatted(catalog, connectionUrl));

        assertRedactedQuery("""
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '***'
                )""".formatted(catalog, connectionUrl), queryEvents);
    }

    @Test
    void testCreateCatalogForNonExistentConnector()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        QueryEvents queryEvents = runQueryAndWaitForEvents("""
                CREATE CATALOG %s USING nonexistent_connector
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )""".formatted(catalog), "No factory for connector 'nonexistent_connector'.*");

        assertRedactedQuery("""
                CREATE CATALOG %s USING nonexistent_connector
                WITH (
                   "user" = '***',
                   "password" = '***'
                )""".formatted(catalog), queryEvents);
    }

    @Test
    void testExplainCreateCatalog()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents("""
                EXPLAIN CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )""".formatted(catalog, connectionUrl));

        assertRedactedQuery("""
                EXPLAIN\s
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '***'
                )""".formatted(catalog, connectionUrl), queryEvents);
    }

    private static void assertRedactedQuery(String expectedQuery, QueryEvents queryEvents)
    {
        QueryCreatedEvent queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertThat(queryCreatedEvent.getMetadata().getQuery()).isEqualTo(expectedQuery);
        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertThat(queryCompletedEvent.getMetadata().getQuery()).isEqualTo(expectedQuery);
    }

    private QueryEvents runQueryAndWaitForEvents(@Language("SQL") String sql)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, getSession()).getQueryEvents();
    }

    private QueryEvents runQueryAndWaitForEvents(@Language("SQL") String sql, String expectedExceptionRegEx)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, getSession(), Optional.of(expectedExceptionRegEx)).getQueryEvents();
    }
}
