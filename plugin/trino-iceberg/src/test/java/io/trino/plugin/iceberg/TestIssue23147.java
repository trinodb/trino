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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static io.trino.SystemSessionProperties.OPTIMIZE_METADATA_QUERIES;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestIssue23147
        extends AbstractTestQueryFramework
{
    private static final String PARTITIONED_TABLE = "partitioned_table";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    @Timeout(30)
    public void test()
    {
        assertUpdate("CREATE TABLE %s(id int, part_key bigint) WITH (partitioning = ARRAY ['part_key'])".formatted(PARTITIONED_TABLE));
        assertUpdate("""
                INSERT INTO %s
                     VALUES
                             (1, 1),
                             (2, 3),
                             (3, 4),
                             (4, 5)
                """.formatted(PARTITIONED_TABLE),
                4);

        Session session = testSessionBuilder(getSession())
                .setSystemProperty(OPTIMIZE_METADATA_QUERIES, "true")
                .build();
        @Language("SQL") String query = """
                     SELECT t0.*
                     FROM (
                             SELECT cast(part_key as varchar) part FROM %s
                             WHERE part_key IN (SELECT part_key FROM %s)
                     ) t0
                     WHERE t0.part IN ('3', '4')
                """.formatted(PARTITIONED_TABLE, PARTITIONED_TABLE);
        assertQuery(session, query, "VALUES ('3'), ('4')");
    }
}
