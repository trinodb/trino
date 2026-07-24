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

import io.trino.tempto.ProductTest;
import io.trino.tests.product.hive.util.TemporaryHiveTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-engine coverage of the DROP COLUMN SerDe allowlist in HiveMetadata. Trino rejects
 * DROP COLUMN for tables whose row-format SerDe is not in
 * {@code HiveMetadata.DROP_COLUMN_SUPPORTED_SERDES} because positional column resolution in
 * those SerDes silently corrupts data on schema change. This test verifies that:
 * <ul>
 *     <li>For unsupported SerDes (ORC, AVRO, JSON, RCBINARY) Trino rejects the operation with
 *         the guard message, and neither Trino nor Hive observes any schema change afterwards.</li>
 *     <li>For supported SerDes (PARQUET, RCTEXT, SEQUENCEFILE, TEXTFILE) Trino performs the
 *         DROP and Hive observes the reduced schema — i.e., Trino's post-drop metastore state
 *         is consistent with what Hive sees.</li>
 * </ul>
 */
public class TestHiveDropColumnSerdeRestriction
        extends ProductTest
{
    @DataProvider(name = "unsupportedSerdeFormats")
    public static Object[][] unsupportedSerdeFormats()
    {
        return new Object[][] {
                {"ORC"},
                {"AVRO"},
                {"JSON"},
                {"RCBINARY"},
        };
    }

    @DataProvider(name = "supportedSerdeFormats")
    public static Object[][] supportedSerdeFormats()
    {
        return new Object[][] {
                {"PARQUET"},
                {"RCTEXT"},
                {"SEQUENCEFILE"},
                {"TEXTFILE"},
        };
    }

    @Test(groups = {STORAGE_FORMATS, PROFILE_SPECIFIC_TESTS}, dataProvider = "unsupportedSerdeFormats")
    public void testTrinoRejectsDropColumnForUnsupportedSerdeFormat(String format)
    {
        String tableName = "test_drop_col_unsupported_" + format.toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName)) {
            onTrino().executeQuery(format(
                    "CREATE TABLE %s (id BIGINT, name VARCHAR, state VARCHAR) WITH (format = '%s')",
                    table.getName(),
                    format));
            onTrino().executeQuery("INSERT INTO " + table.getName() + " VALUES (1, 'Katy', 'CA'), (2, 'Joe', 'WA')");

            // Sanity: both engines see the initial three-column row set.
            assertThat(onTrino().executeQuery("SELECT * FROM " + table.getName()))
                    .containsOnly(row(1, "Katy", "CA"), row(2, "Joe", "WA"));
            assertThat(onHive().executeQuery("SELECT * FROM " + table.getName()))
                    .containsOnly(row(1, "Katy", "CA"), row(2, "Joe", "WA"));

            // Trino's SerDe guard rejects the DROP.
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + table.getName() + " DROP COLUMN state"))
                    .hasMessageContaining("Dropping columns is not supported by table SerDe:");

            // No schema change occurred — the state column is still readable from both engines.
            assertThat(onTrino().executeQuery("SELECT * FROM " + table.getName()))
                    .containsOnly(row(1, "Katy", "CA"), row(2, "Joe", "WA"));
            assertThat(onHive().executeQuery("SELECT * FROM " + table.getName()))
                    .containsOnly(row(1, "Katy", "CA"), row(2, "Joe", "WA"));
        }
    }

    @Test(groups = {STORAGE_FORMATS, PROFILE_SPECIFIC_TESTS}, dataProvider = "supportedSerdeFormats")
    public void testTrinoAndHiveConsistentAfterDropColumnForSupportedSerdeFormat(String format)
    {
        String tableName = "test_drop_col_supported_" + format.toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName)) {
            onTrino().executeQuery(format(
                    "CREATE TABLE %s (id BIGINT, name VARCHAR, state VARCHAR) WITH (format = '%s')",
                    table.getName(),
                    format));
            onTrino().executeQuery("INSERT INTO " + table.getName() + " VALUES (1, 'Katy', 'CA'), (2, 'Joe', 'WA')");

            onTrino().executeQuery("ALTER TABLE " + table.getName() + " DROP COLUMN state");

            // Both engines see the reduced schema (state removed).
            assertThat(onTrino().executeQuery("SELECT * FROM " + table.getName()))
                    .containsOnly(row(1, "Katy"), row(2, "Joe"));
            assertThat(onHive().executeQuery("SELECT * FROM " + table.getName()))
                    .containsOnly(row(1, "Katy"), row(2, "Joe"));
        }
    }
}
