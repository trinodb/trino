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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
public class TestHiveDropColumnSerdeRestriction
{
    static Stream<String> unsupportedSerdeFormats()
    {
        return Stream.of("ORC", "AVRO", "JSON", "RCBINARY");
    }

    static Stream<String> supportedSerdeFormats()
    {
        return Stream.of("PARQUET", "RCTEXT", "SEQUENCEFILE", "TEXTFILE");
    }

    @ParameterizedTest
    @MethodSource("unsupportedSerdeFormats")
    @TestGroup.StorageFormats
    @TestGroup.ProfileSpecificTests
    public void testTrinoRejectsDropColumnForUnsupportedSerdeFormat(String storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_drop_col_unsupported_" + storageFormat.toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %s (id BIGINT, name VARCHAR, state VARCHAR) WITH (format = '%s')",
                    tableName,
                    storageFormat));
            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (1, 'Katy', 'CA'), (2, 'Joe', 'WA')");

            // Sanity: both engines see the initial three-column row set.
            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .containsOnly(row(1L, "Katy", "CA"), row(2L, "Joe", "WA"));
            assertThat(env.executeHive("SELECT * FROM " + tableName))
                    .containsOnly(row(1L, "Katy", "CA"), row(2L, "Joe", "WA"));

            // Trino's SerDe guard rejects the DROP.
            assertThatThrownBy(() -> env.executeTrino("ALTER TABLE " + tableName + " DROP COLUMN state"))
                    .hasMessageContaining("Dropping columns is not supported by table SerDe:");

            // No schema change occurred — the state column is still readable from both engines.
            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .containsOnly(row(1L, "Katy", "CA"), row(2L, "Joe", "WA"));
            assertThat(env.executeHive("SELECT * FROM " + tableName))
                    .containsOnly(row(1L, "Katy", "CA"), row(2L, "Joe", "WA"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("supportedSerdeFormats")
    @TestGroup.StorageFormats
    @TestGroup.ProfileSpecificTests
    public void testTrinoAndHiveConsistentAfterDropColumnForSupportedSerdeFormat(String storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_drop_col_supported_" + storageFormat.toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %s (id BIGINT, name VARCHAR, state VARCHAR) WITH (format = '%s')",
                    tableName,
                    storageFormat));
            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (1, 'Katy', 'CA'), (2, 'Joe', 'WA')");

            env.executeTrinoUpdate("ALTER TABLE " + tableName + " DROP COLUMN state");

            // Both engines see the reduced schema (state removed).
            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .containsOnly(row(1L, "Katy"), row(2L, "Joe"));
            assertThat(env.executeHive("SELECT * FROM " + tableName))
                    .containsOnly(row(1L, "Katy"), row(2L, "Joe"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
