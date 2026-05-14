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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.connector.system.SystemTableHandle;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SystemColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionTablePredicatePushdown
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "schema";
    private File metastoreDir;

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        try {
            metastoreDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        PlanTester planTester = PlanTester.create(session);
        planTester.installPlugin(new TestingIcebergPlugin(metastoreDir.toPath()));
        planTester.createCatalog(CATALOG, "iceberg", ImmutableMap.of());

        HiveMetastore metastore = ((IcebergConnector) planTester.getConnector(CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        return planTester;
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (metastoreDir != null) {
            deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testTopLevelColumn()
    {
        getPlanTester().executeStatement(
                "CREATE TABLE my_table(a, b) WITH (partitioning = ARRAY['b']) AS VALUES (1, 11)");

        assertPlan(
                "SELECT column_name FROM information_schema.columns " +
                        "WHERE table_name = 'my_table$partitions' " +
                        " AND column_name = 'b'",
                anyTree(tableScan("columns")));
    }

    @Test
    public void testPredicatePushdownOnPartitionColumn()
    {
        getPlanTester().executeStatement(
                "CREATE TABLE my_table_filter_pushdown (id, part) " +
                        "WITH (partitioning = ARRAY['part']) " +
                        "AS VALUES (1, '2026-05-07'), (2, '2026-05-08') ");
        Domain expectedDomain = Domain.create(
                SortedRangeSet.copyOf(VarcharType.VARCHAR,
                        List.of(Range.range(VarcharType.VARCHAR,
                                utf8Slice("2026-05-07"),
                                true,
                                utf8Slice("2026-05-08"),
                                true))),
                false
        );

        String sql = "SELECT count(*) " +
                "FROM \"my_table_filter_pushdown$partitions\" " +
                "WHERE part >= '2026-05-07' AND part <= '2026-05-08'";
        SystemTableHandle scanHandle = (SystemTableHandle) getPlanTester().inTransaction(session -> {
            Plan plan = getPlanTester().createPlan(session, sql);
            TableScanNode sc = (TableScanNode) Iterables.getOnlyElement(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof TableScanNode tsn
                            && tsn.getTable().connectorHandle() instanceof SystemTableHandle)
                    .findAll());
            return sc.getTable().connectorHandle();
        });

        TupleDomain<ColumnHandle> constraint = scanHandle.constraint();
        assertThat(constraint.isAll()).isFalse();
        ColumnHandle partCol = constraint.getDomains().orElseThrow().keySet().stream()
                .filter(c -> ((SystemColumnHandle) c).columnName().equals("part"))
                .findFirst()
                .orElseThrow();

        assertThat(constraint.getDomains().orElseThrow().get(partCol))
                .isEqualTo(expectedDomain);
    }
}
