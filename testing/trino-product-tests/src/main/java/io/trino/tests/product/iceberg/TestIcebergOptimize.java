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
package io.trino.tests.product.iceberg;

import io.trino.tempto.ProductTest;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests compatibility between Iceberg connector and Spark Iceberg.
 */
public class TestIcebergOptimize
        extends ProductTest
{
    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testOptimizeTableAfterDelete()
    {
        String baseTableName = "test_optimize_with_small_split_size_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        onTrino().executeQuery("CREATE TABLE " + trinoTableName + "(regionkey integer, country varchar, description varchar) ");

        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES " +
                "(1, 'Poland', 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque rutrum erat faucibus tempus ullamcorper. Duis at auctor est, a accumsan magna.'), " +
                "(1, 'Austria', ' Aliquam rhoncus tortor eu sagittis vulputate. Donec non erat nec dui tempor mattis pellentesque quis sapien. Proin dolor elit, porttitor aliquam erat et, aliquam eleifend elit. '), " +
                "(1, 'France', 'Cras et elit sed nisl faucibus volutpat sed ac sapien. Sed ut metus vulputate, feugiat massa sed, lacinia ipsum. Phasellus ultrices ligula non ultricies gravida'), " +
                "(1, 'Germany', 'Suspendisse eu nunc in lectus blandit pretium posuere non libero. Donec eu ligula hendrerit, pellentesque risus et, luctus odio.'), " +
                "(2, 'United States of America', 'Pellentesque fermentum, tellus eget laoreet aliquam, nibh libero sollicitudin augue, vel posuere tellus odio vel est. Integer ac sem malesuada nibh imperdiet placerat.'), " +
                "(2, 'Canada', 'Pellentesque porta nisi vel viverra sodales. Praesent et magna venenatis, varius quam eget, tristique orci. In ac cursus felis, vel elementum odio.'), " +
                "(3, 'Japan', 'Sed vitae dignissim mi, eu mattis ante. Nam vulputate augue magna, vel viverra diam interdum at. Phasellus vehicula ante sit amet cursus venenatis.')," +
                "(3, 'China', 'Fusce sit amet eleifend nunc. Maecenas bibendum felis felis, eu cursus neque viverra sed. Sed faucibus augue eu placerat elementum. Nam vitae hendrerit odio.')," +
                "(3, 'Laos', 'Nulla vel placerat nibh. Pellentesque a cursus nunc. In tristique sollicitudin vestibulum. Sed imperdiet justo eget rhoncus condimentum. In commodo, purus sit amet malesuada rutrum, neque magna euismod elit')");

        List<String> initialFiles = getActiveFiles(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);

        onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE regionkey = 1");
        onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE regionkey = 2");

        // Verify that delete files exists
        assertThat(
                onTrino().executeQuery(
                        format("SELECT summary['total-delete-files'] FROM %s.%s.\"%s$snapshots\" ", TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName) +
                                "WHERE snapshot_id = " + getCurrentSnapshotId(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName)))
                .containsOnly(row("2"));

        // Set the split size to a small number of bytes so each ORC stripe gets its own split.
        // TODO Drop Spark dependency once that the setting 'read.split.target-size' can be set through Trino
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " SET TBLPROPERTIES ('read.split.target-size'='100')");

        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " EXECUTE OPTIMIZE");

        List<String> updatedFiles = getActiveFiles(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);
        Assertions.assertThat(updatedFiles)
                .hasSize(1)
                .isNotEqualTo(initialFiles);

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                .containsOnly(
                        row(3, "Japan", "Sed vitae dignissim mi, eu mattis ante. Nam vulputate augue magna, vel viverra diam interdum at. Phasellus vehicula ante sit amet cursus venenatis."),
                        row(3, "China", "Fusce sit amet eleifend nunc. Maecenas bibendum felis felis, eu cursus neque viverra sed. Sed faucibus augue eu placerat elementum. Nam vitae hendrerit odio."),
                        row(3, "Laos", "Nulla vel placerat nibh. Pellentesque a cursus nunc. In tristique sollicitudin vestibulum. Sed imperdiet justo eget rhoncus condimentum. In commodo, purus sit amet malesuada rutrum, neque magna euismod elit"));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    private List<String> getActiveFiles(String catalog, String schema, String tableName)
    {
        return onTrino().executeQuery(format("SELECT file_path FROM %s.%s.\"%s$files\"", catalog, schema, tableName))
                .rows().stream()
                .map(row -> (String) row.get(0))
                .collect(toImmutableList());
    }

    private long getCurrentSnapshotId(String catalog, String schema, String tableName)
    {
        return (long) onTrino().executeQuery(format("SELECT snapshot_id FROM %s.%s.\"%s$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", catalog, schema, tableName))
                .getOnlyValue();
    }

    private static String sparkTableName(String tableName)
    {
        return format("%s.%s.%s", SPARK_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private static String trinoTableName(String tableName)
    {
        return format("%s.%s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName);
    }
}
