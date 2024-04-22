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
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergReadVersionedTableByTemporal
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "test-bucket-time-travel";

    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(BUCKET_NAME);

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("fs.hadoop.enabled", "false")
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", minio.getMinioAddress())
                                .put("s3.path-style-access", "true")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .buildOrThrow())
                .build();

        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + ICEBERG_CATALOG + ".tpch");
        return queryRunner;
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        minio = null; // closed by closeAfterClass
    }

    @Test
    public void testSelectTableWithEndVersionAsTemporal()
    {
        String tableName = "test_iceberg_read_versioned_table_" + randomNameSuffix();

        minio.copyResources("iceberg/timetravel", BUCKET_NAME, "timetravel");
        assertUpdate(format(
                "CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')",
                tableName,
                format("s3://%s/timetravel", BUCKET_NAME)));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 1, 2, 3");

        Session utcSession = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        assertThat(query(utcSession, "SELECT made_current_at FROM \"" + tableName + "$history\""))
                .matches("VALUES" +
                        "   TIMESTAMP '2023-06-30 05:01:46.265 UTC'," + // CREATE TABLE timetravel(data integer)
                        "   TIMESTAMP '2023-07-01 05:02:43.954 UTC'," + // INSERT INTO timetravel VALUES 1
                        "   TIMESTAMP '2023-07-02 05:03:39.586 UTC'," + // INSERT INTO timetravel VALUES 2
                        "   TIMESTAMP '2023-07-03 05:03:42.434 UTC'");  // INSERT INTO timetravel VALUES 3

        assertUpdate("INSERT INTO " + tableName + " VALUES 4", 1);

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 2, 3, 4");
        Session viennaSession = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Europe/Vienna")).build();
        Session losAngelesSession = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Los_Angeles")).build();

        // version as date
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-01'"))
                .returnsEmptyResult();
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-01'"))
                .matches("VALUES 1");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-02'"))
                .matches("VALUES 1");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-02'"))
                .matches("VALUES 1, 2");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-03'"))
                .matches("VALUES 1, 2");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-03'"))
                .matches("VALUES 1, 2, 3");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-04'"))
                .matches("VALUES 1, 2, 3");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF DATE '2023-07-04'"))
                .matches("VALUES 1, 2, 3");

        // version as timestamp
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-01 00:00:00'"))
                .returnsEmptyResult();
        assertThat(query(utcSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-01 05:02:43.953'"))
                .returnsEmptyResult();
        assertThat(query(utcSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-01 05:02:43.954'"))
                .matches("VALUES 1");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-01 07:02:43.954'"))
                .matches("VALUES 1");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-01 00:00:00.1'"))
                .matches("VALUES 1");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-02 01:00:00.12'"))
                .matches("VALUES 1");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-02 01:00:00.123'"))
                .matches("VALUES 1, 2");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-03 02:00:00.123'"))
                .matches("VALUES 1, 2");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-03 02:00:00.123456'"))
                .matches("VALUES 1, 2, 3");
        assertThat(query(viennaSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-04 03:00:00.123456789'"))
                .matches("VALUES 1, 2, 3");
        assertThat(query(losAngelesSession, "SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2023-07-04 03:00:00.123456789012'"))
                .matches("VALUES 1, 2, 3");

        assertUpdate("DROP TABLE " + tableName);
    }
}
