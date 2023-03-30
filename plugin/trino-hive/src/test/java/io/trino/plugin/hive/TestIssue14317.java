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
package io.trino.plugin.hive;

import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for https://github.com/trinodb/trino/issues/14317
 * <p>
 * The only known reproduction involves a connector that exposes partitioned tables
 * and supports predicate pushdown.
 */
public class TestIssue14317
{
    @Test
    public void test()
            throws Exception
    {
        HiveQueryRunner.Builder<?> builder = HiveQueryRunner.builder()
                .setCreateTpchSchemas(false);

        try (DistributedQueryRunner queryRunner = builder.build();
                QueryAssertions assertions = new QueryAssertions(queryRunner);) {
            queryRunner.execute("CREATE SCHEMA s");

            queryRunner.execute("CREATE TABLE s.t (a bigint, b bigint)");
            queryRunner.execute("CREATE TABLE s.u (c bigint, d bigint) WITH (partitioned_by = array['d'])");
            queryRunner.execute("CREATE TABLE s.v (e bigint, f bigint)");

            queryRunner.execute("INSERT INTO s.t VALUES (5, 6)");
            queryRunner.execute("INSERT INTO s.u VALUES (5, 6)");
            queryRunner.execute("INSERT INTO s.v VALUES (5, 6)");

            assertThat(assertions.query("""
                    WITH t1 AS (
                      SELECT a
                      FROM (
                        SELECT a, ROW_NUMBER() OVER (PARTITION BY a) AS rn
                        FROM s.t)
                      WHERE rn = 1),
                    t2 AS (SELECT c FROM s.u WHERE d - 5 = 8)
                    SELECT v.e
                    FROM s.v
                      INNER JOIN t1 on v.e = t1.a
                      INNER JOIN t2 ON v.e = t2.c
                    """))
                    .returnsEmptyResult();

            queryRunner.execute("DROP TABLE s.t");
            queryRunner.execute("DROP TABLE s.u");
            queryRunner.execute("DROP TABLE s.v");
            queryRunner.execute("DROP SCHEMA s");
        }
    }
}
