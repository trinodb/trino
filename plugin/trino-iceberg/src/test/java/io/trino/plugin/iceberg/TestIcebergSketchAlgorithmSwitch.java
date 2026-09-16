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
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BlobMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.tpch.TpchTable.NATION;
import static org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Verifies that a table with existing extended statistics keeps its sketch algorithm when
 * {@code iceberg.extended-statistics.ndv-sketch-algorithm} differs. Uses two query runners sharing one
 * warehouse/metastore directory to simulate a config change across catalog restarts.
 */
@TestInstance(PER_CLASS)
final class TestIcebergSketchAlgorithmSwitch
{
    @TempDir
    private static Path dataDir;

    private QueryRunner thetaRunner;
    private QueryRunner hllRunner;

    @BeforeAll
    void setUp()
            throws Exception
    {
        thetaRunner = IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(dataDir))
                .setInitialTables(NATION)
                .build();
        hllRunner = IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(dataDir))
                .addIcebergProperty("iceberg.extended-statistics.ndv-sketch-algorithm", "HLL")
                .disableSchemaInitializer()
                .build();
    }

    @AfterAll
    void tearDown()
    {
        closeAllSuppress(new RuntimeException(), thetaRunner, hllRunner);
    }

    @Test
    void testAnalyzeKeepsExistingAlgorithm()
    {
        String tableName = "test_switch_algorithm_analyze";
        thetaRunner.execute("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation");
        thetaRunner.execute("ANALYZE " + tableName);

        hllRunner.execute("ANALYZE " + tableName);
        assertThat(hllRunner.execute("SELECT count(*) FROM " + tableName).getOnlyValue()).isEqualTo(25L);
        assertThat(blobTypes(tableName)).containsOnly(APACHE_DATASKETCHES_THETA_V1);

        thetaRunner.execute("DROP TABLE " + tableName);
    }

    @Test
    void testInsertKeepsExistingAlgorithm()
    {
        String tableName = "test_switch_algorithm_insert";
        thetaRunner.execute("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation");
        thetaRunner.execute("ANALYZE " + tableName);

        hllRunner.execute("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation");
        assertThat(blobTypes(tableName)).containsOnly(APACHE_DATASKETCHES_THETA_V1);

        thetaRunner.execute("DROP TABLE " + tableName);
    }

    private Set<String> blobTypes(String tableName)
    {
        BaseTable table = loadTable(
                tableName,
                getHiveMetastore(thetaRunner),
                getFileSystemFactory(thetaRunner),
                "iceberg",
                "tpch");
        return table.statisticsFiles().stream()
                .flatMap(file -> file.blobMetadata().stream())
                .map(BlobMetadata::type)
                .collect(toImmutableSet());
    }

    @Test
    void testNoExistingStatsAllowsEitherAlgorithm()
    {
        String tableName = "test_no_existing_stats";
        Session noStatsOnWrite = withStatsOnWrite(thetaRunner.getDefaultSession(), false);
        thetaRunner.execute(noStatsOnWrite, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation");

        // No extended statistics exist yet, so the HLL catalog may freely ANALYZE this table.
        assertThat(hllRunner.execute("ANALYZE " + tableName)).isNotNull();

        thetaRunner.execute("DROP TABLE " + tableName);
    }

    private static Session withStatsOnWrite(Session session, boolean enabled)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.toString(enabled))
                .build();
    }
}
