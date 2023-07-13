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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.AZURE;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobFileSystem
        extends ProductTest
{
    private String schemaLocation;

    @BeforeMethodWithContext
    public void setUp()
    {
        String container = requireNonNull(System.getenv("ABFS_CONTAINER"), "Environment variable not set: ABFS_CONTAINER");
        String account = requireNonNull(System.getenv("ABFS_ACCOUNT"), "Environment variable not set: ABFS_ACCOUNT");
        schemaLocation = format("abfs://%s@%s.dfs.core.windows.net/%s", container, account, "test_" + randomNameSuffix());

        onHive().executeQuery("dfs -rm -f -r " + schemaLocation);
        onHive().executeQuery("dfs -mkdir -p " + schemaLocation);
    }

    @AfterMethodWithContext
    public void tearDown()
    {
        onHive().executeQuery("dfs -mkdir -p " + schemaLocation);
    }

    @Test(groups = AZURE)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testPathContainsSpecialCharacter()
    {
        String tableName = "test_path_special_character" + randomNameSuffix();
        String tableLocation = schemaLocation + "/" + tableName;

        onHive().executeQuery("CREATE TABLE " + tableName + " (id bigint) PARTITIONED BY (part string) LOCATION '" + tableLocation + "'");
        onHive().executeQuery("INSERT INTO " + tableName + " VALUES " +
                "(1, 'hive=equal')," +
                "(2, 'hive+plus')," +
                "(3, 'hive space')," +
                "(4, 'hive:colon')," +
                "(5, 'hive%percent')");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES " +
                "(11, 'trino=equal')," +
                "(12, 'trino+plus')," +
                "(13, 'trino space')," +
                "(14, 'trino:colon')," +
                "(15, 'trino%percent')");

        List<QueryAssert.Row> expected = ImmutableList.of(
                row(1, "hive=equal"),
                row(2, "hive+plus"),
                row(3, "hive space"),
                row(4, "hive:colon"),
                row(5, "hive%percent"),
                row(11, "trino=equal"),
                row(12, "trino+plus"),
                row(13, "trino space"),
                row(14, "trino:colon"),
                row(15, "trino%percent"));

        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);

        onHive().executeQuery("DROP TABLE " + tableName);
    }
}
