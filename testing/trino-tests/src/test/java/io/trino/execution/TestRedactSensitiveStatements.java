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

final class TestRedactSensitiveStatements
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
        queryRunner.installPlugin(new JdbcPlugin("jdbc", TestingH2JdbcModule::new));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);

        return queryRunner;
    }

    @Test
    void testSyntacticallyInvalidStatements()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        QueryEvents createCatalogEvents = runQueryAndWaitForEvents(
                """
                CREATE CCATALOG %s USING jdbc
                WITH (
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )
                """.formatted(catalog), ".*mismatched input 'CCATALOG'.*");

        assertRedactedQuery(
                """
                CREATE CCATALOG %s USING jdbc
                WITH (
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )
                """.formatted(catalog), createCatalogEvents);

        QueryEvents selectEvents = runQueryAndWaitForEvents("SELECTT * FROM nation WHERE name = '\"password\" = ''1234'''", ".*mismatched input 'SELECTT'.*");

        assertRedactedQuery("SELECTT * FROM nation WHERE name = '\"password\" = ''1234'''", selectEvents);
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
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                EXPLAIN ANALYZE CREATE CATALOG %s USING jdbc
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )
                """.formatted(catalog), "EXPLAIN ANALYZE doesn't support statement type: CreateCatalog");

        assertRedactedQuery(
                """
                EXPLAIN ANALYZE CREATE CATALOG %s USING jdbc
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )
                """.formatted(catalog), queryEvents);
    }

    @Test
    void testCreateCatalog()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )
                """.formatted(catalog, connectionUrl));

        assertRedactedQuery(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '***'
                )\
                """.formatted(catalog, connectionUrl), queryEvents);
    }

    @Test
    void testExecuteImmediateCreateCatalog()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                EXECUTE IMMEDIATE
                'CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = ''%s'',
                   "connection-user" = ''bob'',
                   "connection-password" = ''1234''
                )'
                """.formatted(catalog, connectionUrl));

        assertRedactedQuery(
                """
                EXECUTE IMMEDIATE
                'CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = ''%s'',
                   "connection-user" = ''bob'',
                   "connection-password" = ''***''
                )'\
                """.formatted(catalog, connectionUrl), queryEvents);
    }

    @Test
    void testPrepareCreateCatalog()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        QueryEvents prepareEvents = runQueryAndWaitForEvents(
                """
                PREPARE create_catalog FROM
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = ?,
                   "connection-user" = 'bob',
                   "connection-password" = '123'
                )
                """.formatted(catalog));

        // All properties are redacted because we cannot evaluate 'connection-url'.
        assertRedactedQuery(
                """
                PREPARE create_catalog FROM
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '***',
                   "connection-user" = '***',
                   "connection-password" = '***'
                )\
                """.formatted(catalog), prepareEvents);
    }

    @Test
    void testExecutePreparedCreateCatalog()
            throws Exception
    {
        String connectionUrl = createH2ConnectionUrl();
        String catalog = "catalog_" + randomNameSuffix();
        String createCatalog =
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )
                """.formatted(catalog, connectionUrl);

        Session session = Session.builder(getSession())
                .addPreparedStatement("create_catalog", createCatalog)
                .build();
        QueryEvents queryEvents =  queries.runQueryAndWaitForEvents("EXECUTE create_catalog", session).getQueryEvents();

        String redactedCreateCatalog =
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '***'
                )\
                """.formatted(catalog, connectionUrl);
        QueryCreatedEvent queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertThat(queryCreatedEvent.getMetadata().getQuery())
                .isEqualTo("EXECUTE create_catalog");
        assertThat(queryCreatedEvent.getMetadata().getPreparedQuery())
                .isEqualTo(Optional.of(redactedCreateCatalog));

        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertThat(queryCompletedEvent.getMetadata().getQuery())
                .isEqualTo("EXECUTE create_catalog");
        assertThat(queryCompletedEvent.getMetadata().getPreparedQuery())
                .isEqualTo(Optional.of(redactedCreateCatalog));
    }

    @Test
    void testExecutePreparedCreateCatalogWithParameters()
            throws Exception
    {
        String connectionUrl = createH2ConnectionUrl();
        String catalog = "catalog_" + randomNameSuffix();
        String createCatalog =
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = ?
                )
                """.formatted(catalog, connectionUrl);

        Session session = Session.builder(getSession())
                .addPreparedStatement("create_catalog", createCatalog)
                .build();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                "EXECUTE create_catalog USING 'password'",
                session,
                ".*Incorrect number of parameters: expected 0 but found 1");

        // TODO: Currently, dynamic parameters within the WITH clause are not supported, as demonstrated by the query above.
        //       However, the user will only learn about this limitation when they issue the EXECUTE statement.
        //       We should either prevent the use of dynamic parameters during CREATE CATALOG execution
        //       or redact sensitive properties in the EXECUTE statement.
        assertRedactedQuery("EXECUTE create_catalog USING 'password'", queryEvents);
    }

    @Test
    void testCreateCatalogWithDuplicatedProperties()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'foo',
                   "connection-user" = 'bar',
                   "connection-password" = 'password1',
                   "connection-password" = 'password2'
                )
                """.formatted(catalog, connectionUrl));

        assertRedactedQuery(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'foo',
                   "connection-user" = 'bar',
                   "connection-password" = '***',
                   "connection-password" = '***'
                )\
                """.formatted(catalog, connectionUrl), queryEvents);
    }

    @Test
    void testCreateCatalogWithInvalidSecretReference()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'foo',
                   "connection-password" = '${ENV:nonexistent}'
                )
                """.formatted(catalog, connectionUrl), "Environment variable is not set: nonexistent");

        assertRedactedQuery(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '***',
                   "connection-user" = '***',
                   "connection-password" = '***'
                )\
                """.formatted(catalog), queryEvents);
    }

    @Test
    void testCreateCatalogWithNonExistentConnector()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                CREATE CATALOG %s USING nonexistent_connector
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )
                """.formatted(catalog), "No factory for connector 'nonexistent_connector'.*");

        assertRedactedQuery(
                """
                CREATE CATALOG %s USING nonexistent_connector
                WITH (
                   "user" = '***',
                   "password" = '***'
                )\
                """.formatted(catalog), queryEvents);
    }

    @Test
    void testCreateCatalogWithFunctionCall()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = lower('BOB'),
                   "connection-password" = '1234'
                )
                """.formatted(catalog, connectionUrl));

        assertRedactedQuery(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = lower('BOB'),
                   "connection-password" = '***'
                )\
                """.formatted(catalog, connectionUrl), queryEvents);
    }

    @Test
    void testCreateCatalogWithFailingFunctionCall()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = split_part('foo,bar', ',', -1),
                   "connection-password" = '1234'
                )
                """.formatted(catalog, connectionUrl), ".*Invalid value for catalog property 'connection-user'.*");

        assertRedactedQuery(
                """
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '***',
                   "connection-user" = '***',
                   "connection-password" = '***'
                )\
                """.formatted(catalog), queryEvents);
    }

    @Test
    void testExplainCreateCatalog()
            throws Exception
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                """
                EXPLAIN CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '1234'
                )
                """.formatted(catalog, connectionUrl));

        assertRedactedQuery(
                """
                EXPLAIN\s
                CREATE CATALOG %s USING jdbc
                WITH (
                   "connection-url" = '%s',
                   "connection-user" = 'bob',
                   "connection-password" = '***'
                )\
                """.formatted(catalog, connectionUrl), queryEvents);
    }

    private static void assertRedactedQuery(String expectedQuery, QueryEvents queryEvents)
    {
        QueryCreatedEvent queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertThat(queryCreatedEvent.getMetadata().getQuery()).isEqualTo(expectedQuery);
        assertThat(queryCreatedEvent.getMetadata().getPreparedQuery()).isEmpty();
        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertThat(queryCompletedEvent.getMetadata().getQuery()).isEqualTo(expectedQuery);
        assertThat(queryCompletedEvent.getMetadata().getPreparedQuery()).isEmpty();
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

    private QueryEvents runQueryAndWaitForEvents(@Language("SQL") String sql, Session session, String expectedExceptionRegEx)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, session, Optional.of(expectedExceptionRegEx)).getQueryEvents();
    }
}
