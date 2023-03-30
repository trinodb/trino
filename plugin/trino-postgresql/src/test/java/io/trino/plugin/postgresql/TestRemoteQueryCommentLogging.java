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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRemoteQueryCommentLogging
        extends AbstractTestQueryFramework
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        DistributedQueryRunner distributedQueryRunner = createPostgreSqlQueryRunner(
                postgreSqlServer,
                ImmutableMap.of(),
                ImmutableMap.of("query.comment-format", "query executed by $USER"),
                ImmutableList.of(CUSTOMER, NATION));

        return distributedQueryRunner;
    }

    @Test
    public void testShouldLogContextInComment()
    {
        assertThat(postgreSqlServer.recordEventsForOperations(() -> getQueryRunner().execute("CREATE TABLE postgresql.tpch.log_nation_test_table AS (SELECT * FROM postgresql.tpch.nation)"))
                .stopEventsRecording()
                .streamQueriesContaining("\"tpch\".\"tpch\".\"tmp_trino_"))
                .allMatch(query -> query.endsWith("/*query executed by user*/"))
                .size()
                .isGreaterThanOrEqualTo(3); //Depending on whether fault tolerancy is enabled or not, this might vary and we don't want to over-specify

        assertThat(postgreSqlServer.recordEventsForOperations(() -> getQueryRunner().execute("SELECT * FROM postgresql.tpch.log_nation_test_table"))
                .stopEventsRecording()
                .streamQueriesContaining("log_nation_test_table"))
                .allMatch(query -> query.endsWith("/*query executed by user*/"))
                .size()
                .isEqualTo(1);

        assertThat(postgreSqlServer.recordEventsForOperations(() -> getQueryRunner().execute("DELETE FROM postgresql.tpch.log_nation_test_table"))
                .stopEventsRecording()
                .streamQueriesContaining("log_nation_test_table"))
                .allMatch(query -> query.endsWith("/*query executed by user*/"))
                .size()
                .isEqualTo(1);

        assertThat(postgreSqlServer.recordEventsForOperations(() -> getQueryRunner().execute("INSERT INTO postgresql.tpch.log_nation_test_table VALUES (1, 'nation', 1, 'nation')"))
                .stopEventsRecording()
                .streamQueriesContaining("log_nation_test_table", "\"tpch\".\"tpch\".\"tmp_trino_"))
                .allMatch(query -> query.endsWith("/*query executed by user*/"))
                .size()
                .isGreaterThanOrEqualTo(1); //Depending on whether fault tolerancy is enabled or not, this might vary and we don't want to over-specify

        assertThat(postgreSqlServer.recordEventsForOperations(() -> getQueryRunner().execute("DROP TABLE postgresql.tpch.log_nation_test_table"))
                .stopEventsRecording()
                .streamQueriesContaining("log_nation_test_table"))
                .allMatch(query -> query.endsWith("/*query executed by user*/"))
                .size()
                .isEqualTo(1);
    }

    @Test
    public void testShouldLogContextInCommentForTableFunctionsQueryPassthrough()
    {
        assertThat(postgreSqlServer.recordEventsForOperations(() -> getQueryRunner().execute("SELECT * FROM TABLE( postgresql.system.query(query => 'SELECT name FROM tpch.nation WHERE nationkey = 0'))"))
                .stopEventsRecording()
                .streamQueriesContaining("tpch.nation"))
                .allMatch(query -> query.contains("SELECT name FROM tpch.nation WHERE nationkey = 0"))
                .allMatch(query -> query.endsWith("/*query executed by user*/"))
                .size()
                .isEqualTo(1);
    }
}
