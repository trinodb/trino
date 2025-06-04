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
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.testing.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.union;
import static io.trino.plugin.hive.S3Assert.s3Path;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class BaseS3AndGlueMetastoreTest
        extends AbstractTestQueryFramework
{
    private final String partitionByKeyword;
    private final String locationKeyword;

    protected final String bucketName;
    protected final String schemaName = "test_glue_s3_" + randomNameSuffix();

    protected GlueHiveMetastore metastore;
    protected S3Client s3;

    protected BaseS3AndGlueMetastoreTest(String partitionByKeyword, String locationKeyword, String bucketName)
    {
        this.partitionByKeyword = requireNonNull(partitionByKeyword, "partitionByKeyword is null");
        this.locationKeyword = requireNonNull(locationKeyword, "locationKeyword is null");
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
    }

    @BeforeAll
    public void setUp()
    {
        s3 = S3Client.builder().build();
    }

    @AfterAll
    public void tearDown()
    {
        if (metastore != null) {
            metastore.dropDatabase(schemaName, true);
            metastore.shutdown();
            metastore = null;
        }
        if (s3 != null) {
            s3.close();
            s3 = null;
        }
    }

    @Test
    public void testTableNotFound()
    {
        assertThat(query("TABLE non_existent_table_" + randomNameSuffix()))
                .failure().hasMessageMatching("line 1:1: Table '\\w+.test_glue_s3_\\w+.non_existent_table_\\w+' does not exist");

        assertThat(query("SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'non_existent_table_" + randomNameSuffix() + "'"))
                .result().isEmpty();
    }

    @Test
    public void testSchemaNotFound()
    {
        assertThat(query("SHOW TABLES FROM non_existent_schema_" + randomNameSuffix()))
                .failure().hasMessageMatching("line 1:1: Schema 'non_existent_schema_\\w+' does not exist");

        assertThat(query("SELECT * FROM information_schema.tables WHERE table_schema = 'non_existent_schema_" + randomNameSuffix() + "'"))
                .result().isEmpty();
    }

    @Test
    public void testBasicOperationsWithProvidedTableLocation()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            testBasicOperationsWithProvidedTableLocation(false, locationPattern);
            testBasicOperationsWithProvidedTableLocation(true, locationPattern);
        }
    }

    protected void testBasicOperationsWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String tableName = "test_basic_operations_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? "," + partitionByKeyword + " = ARRAY['col_str']" : "");

        String actualTableLocation;
        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                     "WITH (location = '" + location + "'" + partitionQueryPart + ") " +
                     "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");
            actualTableLocation = validateTableLocation(tableName, location);

            assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            assertUpdate("UPDATE " + tableName + " SET col_str = 'other' WHERE col_int = 2", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('other', 2), ('str3', 3), ('str4', 4)");

            assertUpdate("DELETE FROM " + tableName + " WHERE col_int = 3", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('other', 2), ('str4', 4)");

            assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
            validateDataFiles(partitioned ? "col_str" : "", tableName, actualTableLocation);
            validateMetadataFiles(actualTableLocation);
        }
        validateFilesAfterDrop(actualTableLocation);
    }

    @Test
    public void testBasicOperationsWithProvidedSchemaLocation()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            testBasicOperationsWithProvidedSchemaLocation(false, locationPattern);
            testBasicOperationsWithProvidedSchemaLocation(true, locationPattern);
        }
    }

    protected void testBasicOperationsWithProvidedSchemaLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String schemaName = "test_basic_operations_schema_" + randomNameSuffix();
        String schemaLocation = locationPattern.locationForSchema(bucketName, schemaName);
        String tableName = "test_basic_operations_table_" + randomNameSuffix();
        String qualifiedTableName = schemaName + "." + tableName;
        String partitionQueryPart = (partitioned ? "WITH (" + partitionByKeyword + " = ARRAY['col_str'])" : "");

        String actualTableLocation;
        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
        try (UncheckedCloseable ignoredDropSchema = onClose("DROP SCHEMA " + schemaName)) {
            assertThat(getSchemaLocation(schemaName)).isEqualTo(schemaLocation);

            assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_int int, col_str varchar)" + partitionQueryPart);
            try (UncheckedCloseable ignoredDropTable = onClose("DROP TABLE " + qualifiedTableName)) {
                // in case of regular CREATE TABLE, location has generated suffix
                String expectedTableLocationPattern = Pattern.quote(schemaLocation.endsWith("/") ? schemaLocation : schemaLocation + "/") + tableName + "-[a-z0-9]+";
                actualTableLocation = getTableLocation(qualifiedTableName);
                assertThat(actualTableLocation).matches(expectedTableLocationPattern);

                assertUpdate("INSERT INTO " + qualifiedTableName + " (col_str, col_int) VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
                assertQuery("SELECT col_str, col_int FROM " + qualifiedTableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

                assertUpdate("UPDATE " + qualifiedTableName + " SET col_str = 'other' WHERE col_int = 2", 1);
                assertQuery("SELECT col_str, col_int FROM " + qualifiedTableName, "VALUES ('str1', 1), ('other', 2), ('str3', 3)");

                assertUpdate("DELETE FROM " + qualifiedTableName + " WHERE col_int = 3", 1);
                assertQuery("SELECT col_str, col_int FROM " + qualifiedTableName, "VALUES ('str1', 1), ('other', 2)");

                assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
                validateDataFiles(partitioned ? "col_str" : "", qualifiedTableName, actualTableLocation);
                validateMetadataFiles(actualTableLocation);
            }
            assertThat(getTableFiles(actualTableLocation)).isEmpty();
        }
        assertThat(getTableFiles(actualTableLocation)).isEmpty();
    }

    @Test
    public void testMergeWithProvidedTableLocation()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            testMergeWithProvidedTableLocation(false, locationPattern);
            testMergeWithProvidedTableLocation(true, locationPattern);
        }
    }

    protected void testMergeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String tableName = "test_merge_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? "," + partitionByKeyword + " = ARRAY['col_str']" : "");

        String actualTableLocation;
        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                     "WITH (location = '" + location + "'" + partitionQueryPart + ") " +
                     "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            actualTableLocation = validateTableLocation(tableName, location);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

            assertUpdate("MERGE INTO " + tableName + " USING (VALUES 1) t(x) ON false" +
                         " WHEN NOT MATCHED THEN INSERT VALUES ('str4', 4)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

            assertUpdate("MERGE INTO " + tableName + " USING (VALUES 2) t(x) ON col_int = x" +
                         " WHEN MATCHED THEN UPDATE SET col_str = 'other'", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('other', 2), ('str3', 3), ('str4', 4)");

            assertUpdate("MERGE INTO " + tableName + " USING (VALUES 3) t(x) ON col_int = x" +
                         " WHEN MATCHED THEN DELETE", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('other', 2), ('str4', 4)");

            assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
            validateDataFiles(partitioned ? "col_str" : "", tableName, actualTableLocation);
            validateMetadataFiles(actualTableLocation);
        }
        validateFilesAfterDrop(actualTableLocation);
    }

    @Test
    public void testOptimizeWithProvidedTableLocation()
    {
        for (LocationPattern locationPattern : LocationPattern.values()) {
            testOptimizeWithProvidedTableLocation(false, locationPattern);
            testOptimizeWithProvidedTableLocation(true, locationPattern);
        }
    }

    protected void testOptimizeWithProvidedTableLocation(boolean partitioned, LocationPattern locationPattern)
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        String location = locationPattern.locationForTable(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? "," + partitionByKeyword + " = ARRAY['value']" : "");
        String locationQueryPart = locationKeyword + "= '" + location + "'";

        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) " +
                     "WITH (" + locationQueryPart + partitionQueryPart + ")");
        try (UncheckedCloseable ignored = onClose("DROP TABLE " + tableName)) {
            // create multiple data files, INSERT with multiple values would create only one file (if not partitioned)
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'a//double_slash')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'a%percent')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'a//double_slash')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'a///triple_slash')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, 'trailing_slash/')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (7, 'two_trailing_slashes//')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'one')", 1);

            Set<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles).hasSize(8);

            Session session = sessionForOptimize();
            computeActual(session, "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

            assertThat(query("SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY value) FROM " + tableName))
                    .matches("VALUES (BIGINT '39', VARCHAR 'a%percent a///triple_slash a//double_slash a//double_slash one one trailing_slash/ two_trailing_slashes//')");

            Set<String> updatedFiles = getActiveFiles(tableName);
            validateFilesAfterOptimize(getTableLocation(tableName), initialFiles, updatedFiles);
        }
    }

    protected Session sessionForOptimize()
    {
        return getSession();
    }

    protected void validateFilesAfterOptimize(String location, Set<String> initialFiles, Set<String> updatedFiles)
    {
        assertThat(updatedFiles).hasSizeLessThan(initialFiles.size());
        assertThat(getAllDataFilesFromTableDirectory(location)).isEqualTo(union(initialFiles, updatedFiles));
    }

    protected abstract void validateDataFiles(String partitionColumn, String tableName, String location);

    protected abstract void validateMetadataFiles(String location);

    protected String validateTableLocation(String tableName, String expectedLocation)
    {
        String actualTableLocation = getTableLocation(tableName);
        assertThat(actualTableLocation).isEqualTo(expectedLocation);
        return actualTableLocation;
    }

    protected void validateFilesAfterDrop(String location)
    {
        assertThat(getTableFiles(location)).isEmpty();
    }

    protected abstract Set<String> getAllDataFilesFromTableDirectory(String tableLocation);

    protected Set<String> getActiveFiles(String tableName)
    {
        return computeActual("SELECT \"$path\" FROM " + tableName).getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(Collectors.toSet());
    }

    protected String getTableLocation(String tableName)
    {
        return findLocationInQuery("SHOW CREATE TABLE " + tableName);
    }

    protected String getSchemaLocation(String schemaName)
    {
        return metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName))
                .getLocation().orElseThrow(() -> new IllegalArgumentException("Location is empty"));
    }

    private String findLocationInQuery(String query)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual(query).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in" + query + " result");
    }

    protected List<String> getTableFiles(String location)
    {
        Matcher matcher = Pattern.compile("s3://[^/]+/(.+)").matcher(location);
        verify(matcher.matches(), "Does not match [%s]: [%s]", matcher.pattern(), location);
        String fileKey = matcher.group(1);
        return s3.listObjectsV2(request -> request.bucket(bucketName).prefix(fileKey)).contents().stream()
                .map(S3Object::key)
                .map(key -> format("s3://%s/%s", bucketName, key))
                .toList();
    }

    protected UncheckedCloseable onClose(@Language("SQL") String sql)
    {
        requireNonNull(sql, "sql is null");
        return () -> assertUpdate(sql);
    }

    protected String schemaPath()
    {
        return "s3://%s/%s".formatted(bucketName, schemaName);
    }

    protected void verifyPathExist(String path)
    {
        assertThat(s3Path(s3, path)).exists();
    }

    protected enum LocationPattern
    {
        REGULAR("s3://%s/%s/regular/%s"),
        TRAILING_SLASH("s3://%s/%s/trailing_slash/%s/"),
        TWO_TRAILING_SLASHES("s3://%s/%s/two_trailing_slashes/%s//"),
        DOUBLE_SLASH("s3://%s/%s//double_slash/%s"),
        TRIPLE_SLASH("s3://%s/%s///triple_slash/%s"),
        PERCENT("s3://%s/%s/a%%percent/%s"),
        HASH("s3://%s/%s/a#hash/%s"),
        QUESTION_MARK("s3://%s/%s/a?question_mark/%s"),
        WHITESPACE("s3://%s/%s/a whitespace/%s"),
        TRAILING_WHITESPACE("s3://%s/%s/trailing_whitespace/%s "),
        /**/;

        private final String locationPattern;

        LocationPattern(String locationPattern)
        {
            this.locationPattern = requireNonNull(locationPattern, "locationPattern is null");
        }

        public String locationForSchema(String bucketName, String schemaName)
        {
            return locationPattern.formatted(bucketName, "warehouse", schemaName);
        }

        public String locationForTable(String bucketName, String schemaName, String tableName)
        {
            return locationPattern.formatted(bucketName, schemaName, tableName);
        }
    }
}
