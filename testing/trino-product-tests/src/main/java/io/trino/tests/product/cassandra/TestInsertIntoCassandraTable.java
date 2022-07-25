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
package io.trino.tests.product.cassandra;

import io.airlift.units.Duration;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.fulfillment.table.TableName;
import io.trino.tempto.internal.query.CassandraQueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tests.product.TestGroups.CASSANDRA;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.cassandra.DataTypesTableDefinition.CASSANDRA_ALL_TYPES;
import static io.trino.tests.product.cassandra.TestConstants.CONNECTOR_NAME;
import static io.trino.tests.product.cassandra.TestConstants.KEY_SPACE;
import static io.trino.tests.product.utils.QueryAssertions.assertContainsEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestInsertIntoCassandraTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String CASSANDRA_INSERT_TABLE = "Insert_All_Types";
    private static final String CASSANDRA_MATERIALIZED_VIEW = "Insert_All_Types_Mview";

    private Configuration configuration;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        this.configuration = configuration;
        return mutableTable(CASSANDRA_ALL_TYPES, CASSANDRA_INSERT_TABLE, CREATED);
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoValuesToCassandraTableAllSimpleTypes()
    {
        TableName table = mutableTablesState().get(CASSANDRA_INSERT_TABLE).getTableName();
        String tableNameInDatabase = format("%s.%s", CONNECTOR_NAME, table.getNameInDatabase());

        assertContainsEventually(() -> onTrino().executeQuery(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                onTrino().executeQuery(format("SELECT '%s'", table.getSchemalessNameInDatabase())),
                new Duration(1, MINUTES));

        QueryResult queryResult = onTrino().executeQuery("SELECT * FROM " + tableNameInDatabase);
        assertThat(queryResult).hasNoRows();

        // TODO Following types are not supported now. We need to change null into the value after fixing it
        // blob, frozen<set<type>>, inet, list<type>, map<type,type>, set<type>, decimal, varint
        onTrino().executeQuery("INSERT INTO " + tableNameInDatabase +
                "(a, b, bl, bo, d, do, dt, f, fr, i, ti, si, integer, l, m, s, t, ts, tu, u, v, vari) VALUES (" +
                "'ascii value', " +
                "BIGINT '99999', " +
                "null, " +
                "true, " +
                "null, " +
                "123.456789, " +
                "DATE '9999-12-31'," +
                "REAL '123.45678', " +
                "null, " +
                "null, " +
                "TINYINT '-128', " +
                "SMALLINT '-32768', " +
                "123, " +
                "null, " +
                "null, " +
                "null, " +
                "'text value', " +
                "timestamp '9999-12-31 23:59:59Z'," +
                "uuid '50554d6e-29bb-11e5-b345-feff819cdc9f', " +
                "uuid '12151fd2-7586-11e9-8f9e-2a86e4085a59', " +
                "'varchar value'," +
                "null)");

        assertThat(onTrino().executeQuery("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        "ascii value",
                        99999,
                        null,
                        true,
                        null,
                        123.456789,
                        Date.valueOf("9999-12-31"),
                        123.45678,
                        null,
                        null,
                        123,
                        null,
                        null,
                        null,
                        -32768,
                        "text value",
                        -128,
                        Timestamp.from(OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC).toInstant()),
                        "50554d6e-29bb-11e5-b345-feff819cdc9f",
                        "12151fd2-7586-11e9-8f9e-2a86e4085a59",
                        "varchar value",
                        null));

        // insert null for all datatypes
        onTrino().executeQuery("INSERT INTO " + tableNameInDatabase +
                "(a, b, bl, bo, d, do, dt, f, fr, i, ti, si, integer, l, m, s, t, ts, tu, u, v, vari) VALUES (" +
                "'key 1', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null) ");
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s WHERE a = 'key 1'", tableNameInDatabase))).containsOnly(
                row("key 1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

        // insert into only a subset of columns
        onTrino().executeQuery(format("INSERT INTO %s (a, bo, integer, t) VALUES ('key 2', false, 999, 'text 2')", tableNameInDatabase));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s WHERE a = 'key 2'", tableNameInDatabase))).containsOnly(
                row("key 2", null, null, false, null, null, null, null, null, null, 999, null, null, null, null, "text 2", null, null, null, null, null, null));

        // negative test: failed to insert null to primary key
        assertQueryFailure(() -> onTrino().executeQuery(format("INSERT INTO %s (a) VALUES (null) ", tableNameInDatabase)))
                .hasMessageContaining("Invalid null value in condition for column a");
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoValuesToCassandraMaterizedView()
    {
        TableName table = mutableTablesState().get(CASSANDRA_INSERT_TABLE).getTableName();
        onCasssandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW));
        onCasssandra(format("CREATE MATERIALIZED VIEW %s.%s AS " +
                        "SELECT * FROM %s " +
                        "WHERE b IS NOT NULL " +
                        "PRIMARY KEY (a, b) " +
                        "WITH CLUSTERING ORDER BY (integer DESC)",
                KEY_SPACE,
                CASSANDRA_MATERIALIZED_VIEW,
                table.getNameInDatabase()));

        assertContainsEventually(() -> onTrino().executeQuery(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                onTrino().executeQuery(format("SELECT lower('%s')", CASSANDRA_MATERIALIZED_VIEW)),
                new Duration(1, MINUTES));

        assertQueryFailure(() -> onTrino().executeQuery(format("INSERT INTO %s.%s.%s (a) VALUES (null) ", CONNECTOR_NAME, KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW)))
                .hasMessageContaining("Inserting into materialized views not yet supported");

        assertQueryFailure(() -> onTrino().executeQuery(format("DROP TABLE %s.%s.%s", CONNECTOR_NAME, KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW)))
                .hasMessageContaining("Dropping materialized views not yet supported");

        onCasssandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoTupleType()
    {
        String tableName = "insert_tuple_table";

        onCasssandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));

        onCasssandra(format("CREATE TABLE %s.%s (key int, value frozen<tuple<int, text, float>>, PRIMARY KEY (key))",
                 KEY_SPACE, tableName));

        assertQueryFailure(() -> onTrino().executeQuery(format("INSERT INTO %s.%s.%s (key, value) VALUES (1, ROW(1, 'text-1', 1.11))", CONNECTOR_NAME, KEY_SPACE, tableName)))
                .hasMessageContaining("Unsupported column type: row(integer, varchar, real)");

        onCasssandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));
    }

    private void onCasssandra(String query)
    {
        CassandraQueryExecutor queryExecutor = new CassandraQueryExecutor(configuration);
        queryExecutor.executeQuery(query);
        queryExecutor.close();
    }
}
