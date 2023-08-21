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
import io.trino.filesystem.Location;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Iceberg connector test ORC and with S3-compatible storage (but without real metastore).
 */
public class TestIcebergMinioOrcConnectorTest
        extends BaseIcebergConnectorTest
{
    private final String bucketName = "test-iceberg-orc-" + randomNameSuffix();

    public TestIcebergMinioOrcConnectorTest()
    {
        super(ORC);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Minio minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", minio.getMinioAddress())
                                .put("s3.path-style-access", "true")
                                .put("s3.streaming.part-size", "5MB") // minimize memory usage
                                .put("s3.max-connections", "2") // verify no leaks
                                .put("iceberg.register-table-procedure.enabled", "true")
                                // Allows testing the sorting writer flushing to the file system with smaller tables
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName("tpch")
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/iceberg_data/tpch'"))
                                .build())
                .build();
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary") &&
                !typeName.equalsIgnoreCase("uuid");
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystem, Location.of(path), sortColumnName);
    }

    @Test
    public void testTinyintType()
            throws Exception
    {
        testReadSingleIntegerColumnOrcFile("single-tinyint-column.orc", 127);
    }

    @Test
    public void testSmallintType()
            throws Exception
    {
        testReadSingleIntegerColumnOrcFile("single-smallint-column.orc", 32767);
    }

    private void testReadSingleIntegerColumnOrcFile(String orcFileResourceName, int expectedValue)
            throws Exception
    {
        checkArgument(expectedValue != 0);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_read_as_integer", "(\"_col0\") AS VALUES 0, NULL")) {
            String orcFilePath = (String) computeScalar(format("SELECT DISTINCT file_path FROM \"%s$files\"", table.getName()));
            try (OutputStream outputStream = fileSystem.newOutputFile(Location.of(orcFilePath)).createOrOverwrite()) {
                Files.copy(new File(getResource(orcFileResourceName).toURI()).toPath(), outputStream);
            }
            fileSystem.deleteFiles(List.of(Location.of(orcFilePath.replaceAll("/([^/]*)$", ".$1.crc"))));

            Session ignoreFileSizeFromMetadata = Session.builder(getSession())
                    // The replaced and replacing file sizes may be different
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "use_file_size_from_metadata", "false")
                    .build();
            assertThat(query(ignoreFileSizeFromMetadata, "TABLE " + table.getName()))
                    .matches("VALUES NULL, " + expectedValue);
        }
    }

    @Test
    public void testTimeType()
    {
        // Regression test for https://github.com/trinodb/trino/issues/15603
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_time", "(col time(6))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (TIME '13:30:00'), (TIME '14:30:00'), (NULL)", 3);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES '13:30:00', '14:30:00', NULL");
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    """
                            VALUES
                            ('col', null, 2.0, 0.33333333333, null, null, null),
                            (null, null, null, null, 3, null, null)
                            """);
        }
    }

    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        // TODO https://github.com/trinodb/trino/issues/16273 The connector can't read row types having ambiguous field names in ORC files. e.g. row(X int, x int)
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessageContaining("Error opening Iceberg split")
                .hasStackTraceContaining("Multiple entries with same key");
    }
}
