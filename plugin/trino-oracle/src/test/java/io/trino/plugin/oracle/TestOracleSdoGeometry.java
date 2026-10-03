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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Reproduces trinodb/trino#30020: MDSYS.SDO_GEOMETRY columns fail to surface through the
 * Trino Oracle connector. Currently, the connector drops SDO_GEOMETRY columns (Optional.empty
 * from toColumnMapping), and enabling unsupported-type-handling=CONVERT_TO_VARCHAR raises an
 * INTERNAL_ERROR whose message is "oracle/xdb/XMLType" — the JVM class name from a
 * NoClassDefFoundError inside the Oracle JDBC driver's OPAQUE handler.
 *
 * <p>This test asserts the post-fix behavior: an SDO_GEOMETRY column materializes as a
 * VARCHAR containing the geometry's WKT representation (via server-side SDO_UTIL.TO_WKTGEOMETRY).
 * Until the plugin change lands, this test is expected to fail — either because the geom
 * column is not resolvable, or because reading it raises the XMLType error.
 *
 * <p>Uses the full (non-slim) gvenzl/oracle-free image via {@link TestingOracleSpatialServer};
 * Oracle Spatial (which provides MDSYS.SDO_GEOMETRY) is stripped from the slim variant.
 */
@TestInstance(PER_CLASS)
public class TestOracleSdoGeometry
        extends AbstractTestQueryFramework
{
    private TestingOracleSpatialServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleSpatialServer();
        return OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.connection-pool.enabled", "false")
                        .buildOrThrow())
                .build();
    }

    @AfterAll
    public final void destroy()
            throws Exception
    {
        Closeables.closeAll(oracleServer);
        oracleServer = null;
    }

    @Test
    public void testSelectSdoGeometryAsWkt()
    {
        String tableName = "test_sdo_geom_" + randomNameSuffix();
        String qualifiedName = TEST_SCHEMA + "." + tableName;
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER(10), geom MDSYS.SDO_GEOMETRY)", qualifiedName));
        try {
            // 2D point (1.0, 2.0). SDO_GTYPE=2001 (2-dimensional point).
            oracleServer.execute(format(
                    "INSERT INTO %s (id, geom) VALUES (1, MDSYS.SDO_GEOMETRY(2001, NULL, MDSYS.SDO_POINT_TYPE(1.0, 2.0, NULL), NULL, NULL))",
                    qualifiedName));

            MaterializedResult result = computeActual(format("SELECT id, geom FROM %s", tableName));
            assertThat(result.getRowCount()).isEqualTo(1);

            MaterializedRow row = result.getMaterializedRows().getFirst();
            assertThat(row.getField(0)).isEqualTo(new BigDecimal("1"));
            assertThat((String) row.getField(1))
                    .as("SDO_GEOMETRY column should be surfaced as WKT once the plugin fix is applied")
                    .isNotNull()
                    .containsIgnoringCase("POINT")
                    .contains("1")
                    .contains("2");
        }
        finally {
            oracleServer.execute("DROP TABLE " + qualifiedName);
        }
    }

    /**
     * Verifies the {@code oracle.spatial-column-mapping=GEOMETRY} config path: the connector
     * parses the server-side WKT and hands Trino a native GEOMETRY column, so spatial functions
     * like {@code ST_AsText} apply directly (no {@code ST_GeometryFromText(varchar)} wrapping).
     * Uses a locally-scoped QueryRunner because the config property is bound at connector
     * construction time — it can't be flipped per query on the class-level runner.
     */
    @Test
    public void testSelectSdoGeometryAsNativeType()
            throws Exception
    {
        String tableName = "test_sdo_geom_native_" + randomNameSuffix();
        String qualifiedName = TEST_SCHEMA + "." + tableName;
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER(10), geom MDSYS.SDO_GEOMETRY)", qualifiedName));
        try (QueryRunner nativeRunner = OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.connection-pool.enabled", "false")
                        .put("oracle.spatial-column-mapping", "GEOMETRY")
                        .buildOrThrow())
                .build()) {
            oracleServer.execute(format(
                    "INSERT INTO %s (id, geom) VALUES (1, MDSYS.SDO_GEOMETRY(2001, NULL, MDSYS.SDO_POINT_TYPE(1.0, 2.0, NULL), NULL, NULL))",
                    qualifiedName));

            // ST_AsText only compiles against a GEOMETRY column; success here proves the column
            // is truly native geometry, not a VARCHAR that happens to hold WKT.
            MaterializedResult result = nativeRunner.execute(
                    nativeRunner.getDefaultSession(),
                    format("SELECT id, ST_AsText(geom) FROM %s", tableName));
            assertThat(result.getRowCount()).isEqualTo(1);
            MaterializedRow row = result.getMaterializedRows().getFirst();
            assertThat(row.getField(0)).isEqualTo(new BigDecimal("1"));
            assertThat((String) row.getField(1))
                    .as("ST_AsText should round-trip the geometry")
                    .isEqualTo("POINT (1 2)");
        }
        finally {
            oracleServer.execute("DROP TABLE " + qualifiedName);
        }
    }
}
