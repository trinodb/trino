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

import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

/**
 * Test for https://github.com/trinodb/trino/issues/14317
 *
 * The only known reproduction involves a connector that exposes partitioned tables
 * and supports predicate pushdown.
 */
public class TestIssue14317
        extends AbstractTestQueryFramework
{
    private HiveMinioDataLake minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(new HiveMinioDataLake("test-issue14317"));
        minio.start();

        return S3HiveQueryRunner.builder(minio).build();
    }

    @Test
    public void test()
    {
        assertUpdate("CREATE SCHEMA s");

        assertUpdate("CREATE TABLE s.t (a bigint, b bigint)");
        assertUpdate("CREATE TABLE s.u (c bigint, d bigint) WITH (partitioned_by = array['d'])");
        assertUpdate("CREATE TABLE s.v (e bigint, f bigint)");

        computeActual("INSERT INTO s.t VALUES (5, 6)");
        computeActual("INSERT INTO s.u VALUES (5, 6)");
        computeActual("INSERT INTO s.v VALUES (5, 6)");

        assertQuerySucceeds("""
            WITH t1 AS (
              SELECT a
              FROM (
                SELECT a, ROW_NUMBER() OVER (PARTITION BY a) AS rn
                FROM s.t
              )
              WHERE rn = 1
            ),
            t2 AS (SELECT c FROM s.u WHERE d - 5 = 8)
            SELECT v.e
            FROM s.v
              INNER JOIN t1 on v.e = t1.a
              INNER JOIN t2 ON v.e = t2.c;
            """);

        assertUpdate("DROP TABLE s.t");
        assertUpdate("DROP TABLE s.u");
        assertUpdate("DROP TABLE s.v");
        assertUpdate("DROP SCHEMA IF EXISTS s");
    }
}
