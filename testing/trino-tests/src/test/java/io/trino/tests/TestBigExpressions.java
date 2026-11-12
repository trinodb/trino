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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static java.nio.file.Files.createDirectories;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigExpressions
        extends AbstractTestQueryFramework
{
    private static final String PROJECTION_TOO_COMPLEX = "Failed to execute query; there may be too many columns used or expressions are too complex";
    private static final String FILTER_TOO_COMPLEX = "Query exceeded maximum filters\\. Please reduce the number of filters referenced and re-run the query\\.";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = TpchQueryRunner.builder().setWorkerCount(1).build();
        try {
            Path dataDirectory = createDirectories(queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));
            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.createCatalog("iceberg", "iceberg", ImmutableMap.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///",
                    "fs.local.enabled", "true",
                    "local.location", dataDirectory.toString()));
            queryRunner.execute("CREATE SCHEMA iceberg.default");
            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    @Test
    public void testMergeIntoWideTable()
    {
        // MERGE plans as target RIGHT JOIN source, which puts every source column on the join build side
        int columnCount = 1500;
        StringJoiner columns = new StringJoiner(", ");
        StringJoiner sourceValues = new StringJoiner(", ");
        StringJoiner updates = new StringJoiner(", ");
        StringJoiner insertColumns = new StringJoiner(", ");
        StringJoiner insertValues = new StringJoiner(", ");
        for (int i = 0; i < columnCount; i++) {
            columns.add("c%s varchar".formatted(i));
            sourceValues.add("CAST(id * %s AS varchar) AS c%s".formatted(i, i));
            updates.add("c%1$s = s.c%1$s".formatted(i));
            insertColumns.add("c" + i);
            insertValues.add("s.c" + i);
        }
        assertUpdate("CREATE TABLE iceberg.default.wide_target (id bigint, %s)".formatted(columns));
        assertUpdate("CREATE TABLE iceberg.default.wide_source (id bigint, %s)".formatted(columns));
        assertUpdate("INSERT INTO iceberg.default.wide_target (id, c0, c1) VALUES (1, 'old', 'old'), (2, 'old', 'old')", 2);
        assertUpdate("INSERT INTO iceberg.default.wide_source SELECT id, %s FROM UNNEST(sequence(2, 3)) t(id)".formatted(sourceValues), 2);

        // a partitioned join keeps the syntactic join order, which makes the wide source the build side
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
        assertUpdate(
                session,
                """
                MERGE INTO iceberg.default.wide_target t USING iceberg.default.wide_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET %s
                WHEN NOT MATCHED THEN INSERT (id, %s) VALUES (s.id, %s)
                """.formatted(updates, insertColumns, insertValues),
                2);

        assertQuery(
                "SELECT id, c0, c1, c%1$s FROM iceberg.default.wide_target".formatted(columnCount - 1),
                "VALUES (1, 'old', 'old', NULL), (2, '0', '2', '%s'), (3, '0', '3', '%s')".formatted(2 * (columnCount - 1), 3 * (columnCount - 1)));
        assertThat(query("SELECT id, %s FROM iceberg.default.wide_target WHERE id IN (2, 3)".formatted(insertColumns)))
                .matches("SELECT id, %s FROM UNNEST(sequence(2, 3)) t(id)".formatted(sourceValues));

        assertUpdate("DROP TABLE iceberg.default.wide_target");
        assertUpdate("DROP TABLE iceberg.default.wide_source");
    }

    @Test
    public void testComplexSwitchInProjection()
    {
        assertQueryFails("SELECT %s FROM nation".formatted(generateCase("comment", 5, 5)), PROJECTION_TOO_COMPLEX);
    }

    @Test
    public void testComplexSwitchInFilter()
    {
        assertQueryFails("SELECT nationkey FROM nation WHERE %s > 0".formatted(generateCase("comment", 5, 5)), FILTER_TOO_COMPLEX);
    }

    /**
     * Builds a nested CASE with {@code whenCases ^ (depth + 1)} branches, each holding a distinct literal
     * so that no clause is folded away before compilation.
     */
    private static String generateCase(String column, int whenCases, int depth)
    {
        return generateCase(column, whenCases, depth, new AtomicInteger());
    }

    private static String generateCase(String column, int whenCases, int depth, AtomicInteger literals)
    {
        StringBuilder builder = new StringBuilder("CASE ");
        for (int i = 0; i < whenCases; i++) {
            String result;
            if (depth == 0) {
                result = String.valueOf(literals.getAndIncrement());
            }
            else {
                result = generateCase(column, whenCases, depth - 1, literals);
            }
            builder.append(" WHEN %s IN ('%s') THEN (%s)".formatted(column, literals.getAndIncrement(), result));
        }
        return builder.append(" ELSE -1 END").toString();
    }
}
