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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static io.trino.cli.ClientOptions.OutputFormat.ALIGNED;
import static io.trino.cli.ClientOptions.OutputFormat.CSV_HEADER;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TestQueryRunner.createQueryRunner;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the CLI degrades gracefully with a driver other than Trino's.
 */
public class TestGenericDriverQueryRunner
{
    @Test
    public void testExecuteStatements()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner(createConnectionFactory())) {
            assertThat(queryRunner.getSchema()).hasValue("PUBLIC");

            assertThat(execute(queryRunner, "CREATE TABLE test (id BIGINT, name VARCHAR(10), payload VARBINARY(4), tags INTEGER ARRAY, created TIMESTAMP(9))", CSV_HEADER, false))
                    .containsExactly("true", "", "0 rows\n");
            assertThat(execute(queryRunner, "INSERT INTO test VALUES (1, 'alice', X'0102', ARRAY[1, 2], TIMESTAMP '2020-01-02 03:04:05.123456789'), (2, NULL, NULL, NULL, NULL)", CSV_HEADER, false))
                    .containsExactly("true", "", "2 rows\n");

            assertThat(execute(queryRunner, "SELECT * FROM test ORDER BY id", ALIGNED, true))
                    .containsExactly(
                            "true",
                            "" +
                                    " ID | NAME  | PAYLOAD |  TAGS  |            CREATED            \n" +
                                    "----+-------+---------+--------+-------------------------------\n" +
                                    "  1 | alice | 01 02   | [1, 2] | 2020-01-02 03:04:05.123456789 \n" +
                                    "  2 | NULL  | NULL    | NULL   | NULL                          \n" +
                                    "(2 rows)\n",
                            "");

            List<String> failure = execute(queryRunner, "SELECT * FROM no_such_table", CSV_HEADER, false);
            assertThat(failure.get(0)).isEqualTo("false");
            assertThat(failure.get(2)).contains("NO_SUCH_TABLE");
        }
    }

    @Test
    public void testMetadataConnection()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner(createConnectionFactory())) {
            execute(queryRunner, "CREATE TABLE first_table (id BIGINT)", CSV_HEADER, false);
            execute(queryRunner, "CREATE TABLE second_table (id BIGINT)", CSV_HEADER, false);

            List<String> tables = queryRunner.withMetadataConnection(connection -> {
                DatabaseMetaData metadata = connection.getMetaData();
                ImmutableList.Builder<String> names = ImmutableList.builder();
                try (ResultSet resultSet = metadata.getTables(null, "PUBLIC", null, null)) {
                    while (resultSet.next()) {
                        names.add(resultSet.getString("TABLE_NAME"));
                    }
                }
                return names.build();
            });
            assertThat(tables).containsExactlyInAnyOrder("FIRST_TABLE", "SECOND_TABLE");
        }
    }

    private static List<String> execute(QueryRunner queryRunner, String sql, ClientOptions.OutputFormat outputFormat, boolean showProgress)
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        boolean success;
        try (Query query = queryRunner.startQuery(sql)) {
            success = query.renderOutput(getTerminal(), new PrintStream(out, true, UTF_8), new PrintStream(err, true, UTF_8), outputFormat, Optional.of(""), showProgress, false);
        }
        return ImmutableList.of(String.valueOf(success), out.toString(UTF_8), err.toString(UTF_8));
    }

    private static ConnectionFactory createConnectionFactory()
            throws SQLException
    {
        // the database is kept as long as any connection uses it
        String url = "jdbc:h2:mem:test_" + randomUUID().toString().replace("-", "");
        return DriverConnectionFactory.builder(url).build();
    }
}
