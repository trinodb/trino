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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Verifies that switching {@code iceberg.extended-statistics.ndv-sketch-algorithm} for a table which
 * already has extended statistics written under a different algorithm is rejected, rather than silently
 * resetting or ignoring the mismatched statistics. Uses two separate query runners sharing one on-disk
 * warehouse/metastore directory, one per algorithm, to simulate a config change across catalog restarts.
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
    void testSwitchingAlgorithmOnAnalyzeIsRejected()
    {
        String tableName = "test_switch_algorithm_analyze";
        thetaRunner.execute("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation");
        thetaRunner.execute("ANALYZE " + tableName);

        assertThatThrownBy(() -> hllRunner.execute("ANALYZE " + tableName))
                .hasMessageContaining("different sketch algorithm")
                .hasMessageContaining("ndv-sketch-algorithm");

        thetaRunner.execute("DROP TABLE " + tableName);
    }

    @Test
    void testSwitchingAlgorithmOnInsertIsRejected()
    {
        String tableName = "test_switch_algorithm_insert";
        thetaRunner.execute("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation");
        thetaRunner.execute("ANALYZE " + tableName);

        assertThatThrownBy(() -> hllRunner.execute("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation"))
                .hasMessageContaining("different sketch algorithm")
                .hasMessageContaining("ndv-sketch-algorithm");

        thetaRunner.execute("DROP TABLE " + tableName);
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
