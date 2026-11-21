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

import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveFileFormatExtension
        extends AbstractTestQueryFramework
{
    private static final String DEFAULT_CATALOG = "hive";
    private static final String EXTENSION_CATALOG = "hive_with_extension";
    private static final String SCHEMA = "test_suffix";

    private Path dataDirectory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("hive-data");
        closeAfterClass(() -> deleteRecursively(dataDirectory, ALLOW_INSECURE));
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(DEFAULT_CATALOG)
                        .setSchema(SCHEMA)
                        .build())
                .setWorkerCount(0)
                .build();
        try {
            queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
            queryRunner.createCatalog(DEFAULT_CATALOG, "hive", Map.of("hive.create-empty-bucket-files", "true"));
            queryRunner.createCatalog(EXTENSION_CATALOG, "hive", Map.of(
                    "hive.create-empty-bucket-files", "true",
                    "hive.write-file-format-extension", "true"));
            for (String catalog : List.of(DEFAULT_CATALOG, EXTENSION_CATALOG)) {
                queryRunner.execute("CREATE SCHEMA " + catalog + "." + SCHEMA);
            }
            return queryRunner;
        }
        catch (Throwable t) {
            queryRunner.close();
            throw t;
        }
    }

    @ParameterizedTest
    @CsvSource({
            "ORC, GZIP, '', .orc",
            "PARQUET, GZIP, '', .parquet",
            "AVRO, GZIP, '', .avro",
            "CSV, NONE, '', .csv",
            "CSV, GZIP, .gz, .csv.gz",
            "JSON, NONE, '', .json",
            "JSON, GZIP, .gz, .json.gz",
            "OPENX_JSON, NONE, '', .json",
            "TEXTFILE, NONE, '', .txt",
            "TEXTFILE, GZIP, .gz, .txt.gz",
    })
    void testFileExtension(HiveStorageFormat format, HiveCompressionOption codec, String defaultExtension, String enabledExtension)
    {
        for (String catalog : List.of(DEFAULT_CATALOG, EXTENSION_CATALOG)) {
            Session session = Session.builder(getSession())
                    .setCatalog(catalog)
                    .setCatalogSessionProperty(catalog, "compression_codec", codec.name())
                    .build();
            try (TestTable table = new TestTable(
                    sql -> getQueryRunner().execute(session, sql),
                    "test_file_extension_",
                    "WITH (format = '%s') AS SELECT CAST('value' AS varchar) AS data".formatted(format))) {
                String path = (String) computeScalar(session, "SELECT \"$path\" FROM " + table.getName() + " WHERE data = 'value'");
                assertThat(fileExtension(Location.of(path).fileName()))
                        .isEqualTo(catalog.equals(DEFAULT_CATALOG) ? defaultExtension : enabledExtension);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            DEFAULT_CATALOG + ", .gz, ''",
            EXTENSION_CATALOG + ", .txt.gz, .txt",
    })
    void testEmptyBucketFileExtension(String catalog, String dataExtension, String emptyExtension)
            throws IOException
    {
        Session session = Session.builder(getSession()).setCatalog(catalog).build();
        try (TestTable table = new TestTable(sql -> getQueryRunner().execute(session, sql), "test_empty_bucket_extension_",
                """
                WITH (format = 'TEXTFILE', bucketed_by = ARRAY['bucket_key'], bucket_count = 2)
                AS SELECT 1 AS bucket_key
                """)) {
            Path dataFile = getBucketDataFile(session, table.getName(), 1);
            assertBucketFileExtensions(dataFile, Set.of(), dataExtension, emptyExtension);
        }
    }

    @Test
    void testEmptyBucketFileExtensionUsesExistingPartitionFormat()
            throws IOException
    {
        Session session = Session.builder(getSession()).setCatalog(EXTENSION_CATALOG).build();
        Session createSession = Session.builder(session)
                .setCatalogSessionProperty(EXTENSION_CATALOG, "respect_table_format", "false")
                .setCatalogSessionProperty(EXTENSION_CATALOG, "hive_storage_format", "TEXTFILE")
                .build();
        try (TestTable table = new TestTable(sql -> getQueryRunner().execute(createSession, sql), "test_partition_extension_",
                """
                WITH (format = 'PARQUET', partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket_key'], bucket_count = 2)
                AS SELECT 1 AS bucket_key, 'p' AS part
                """)) {
            Path ctasFile = getBucketDataFile(session, table.getName(), 1);
            Set<Path> ctasFiles = listDataFiles(ctasFile.getParent());
            assertUpdate(session, "INSERT INTO " + table.getName() + " VALUES (2, 'p')", 1);
            Path insertFile = getBucketDataFile(session, table.getName(), 2);
            assertBucketFileExtensions(insertFile, ctasFiles, ".txt.gz", ".txt");
        }
    }

    private Path getBucketDataFile(Session session, String tableName, int bucketKey)
    {
        String path = (String) computeScalar(session, "SELECT \"$path\" FROM %s WHERE bucket_key = %s".formatted(tableName, bucketKey));
        return dataDirectory.resolve(Location.of(path).path());
    }

    private static void assertBucketFileExtensions(Path dataFile, Set<Path> previousFiles, String dataExtension, String emptyExtension)
            throws IOException
    {
        assertThat(fileExtension(dataFile.getFileName().toString())).isEqualTo(dataExtension);
        assertThat(difference(listDataFiles(dataFile.getParent()), previousFiles))
                .contains(dataFile)
                .map(file -> fileExtension(file.getFileName().toString()))
                .containsExactlyInAnyOrder(dataExtension, emptyExtension);
    }

    private static Set<Path> listDataFiles(Path directory)
            throws IOException
    {
        try (var files = Files.list(directory)) {
            return files.filter(file -> !file.getFileName().toString().startsWith(".")).collect(toImmutableSet());
        }
    }

    private static String fileExtension(String fileName)
    {
        int index = fileName.indexOf('.');
        return index < 0 ? "" : fileName.substring(index);
    }
}
