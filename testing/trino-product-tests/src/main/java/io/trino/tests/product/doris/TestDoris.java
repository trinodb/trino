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
package io.trino.tests.product.doris;

import io.airlift.units.Duration;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.DORIS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onDoris;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDoris
        extends ProductTest
{
    private static final Duration METADATA_VISIBILITY_TIMEOUT = new Duration(1, MINUTES);

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testReadFromDorisTable()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS test");
        onDoris().executeQuery("DROP TABLE IF EXISTS test.nation");

        try {
            onDoris().executeQuery(
                    """
                    CREATE TABLE test.nation (
                        nationkey INT,
                        name VARCHAR(25),
                        regionkey INT
                    )
                    DUPLICATE KEY(nationkey)
                    DISTRIBUTED BY HASH(nationkey) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """);
            onDoris().executeQuery(
                    """
                    INSERT INTO test.nation VALUES
                        (0, 'ALGERIA', 0),
                        (8, 'INDIA', 2),
                        (24, 'UNITED STATES', 1)
                    """);

            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM doris"))
                        .contains(row("test"));
                assertThat(onTrino().executeQuery("SHOW TABLES FROM doris.test"))
                        .contains(row("nation"));
                assertThat(onTrino().executeQuery("SELECT name FROM doris.test.nation WHERE nationkey = 8"))
                        .containsOnly(row("INDIA"));
                assertThat(onTrino().executeQuery("SELECT regionkey, count(*) FROM doris.test.nation GROUP BY regionkey"))
                        .containsOnly(row(0, 1), row(1, 1), row(2, 1));
            });
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS test.nation");
        }
    }

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testDateTimeV2AndLargeintMappings()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS test");
        onDoris().executeQuery("DROP TABLE IF EXISTS test.type_mapping");

        try {
            onDoris().executeQuery(
                    """
                    CREATE TABLE test.type_mapping (
                        user_id BIGINT,
                        created_at DATETIMEV2(3),
                        large_id LARGEINT
                    )
                    DUPLICATE KEY(user_id)
                    DISTRIBUTED BY HASH(user_id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """);
            onDoris().executeQuery(
                    """
                    INSERT INTO test.type_mapping VALUES
                        (1001, '2026-03-31 09:15:01.123', CAST('123456789012345678901234567801' AS LARGEINT))
                    """);

            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW COLUMNS FROM doris.test.type_mapping"))
                        .contains(row("user_id", "bigint", "", ""))
                        .contains(row("created_at", "timestamp(3)", "", ""))
                        .contains(row("large_id", "varchar", "", ""));
                assertThat(onTrino().executeQuery("SELECT CAST(created_at AS VARCHAR), large_id FROM doris.test.type_mapping"))
                        .containsOnly(row("2026-03-31 09:15:01.123", "123456789012345678901234567801"));
            });
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS test.type_mapping");
        }
    }

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testVisibleEmptySchemasExcludeInternalSchemas()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS empty_schema_visibility");

        try {
            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM doris").column(1))
                        .contains("empty_schema_visibility")
                        .doesNotContain("__internal_schema", "mysql");
            });
        }
        finally {
            onDoris().executeQuery("DROP DATABASE IF EXISTS empty_schema_visibility");
        }
    }

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testBooleanAndTimestampMappings()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS test");
        onDoris().executeQuery("DROP TABLE IF EXISTS test.type_mapping_extended");

        try {
            onDoris().executeQuery(
                    """
                    CREATE TABLE test.type_mapping_extended (
                        id BIGINT,
                        bool_col BOOLEAN,
                        created_at DATETIME,
                        created_at_millis DATETIMEV2(3),
                        created_at_micros DATETIMEV2(6)
                    )
                    DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """);
            onDoris().executeQuery(
                    """
                    INSERT INTO test.type_mapping_extended VALUES
                        (1, true, '2026-03-31 09:15:01', '2026-03-31 09:15:01.123', '2026-03-31 09:15:01.123456'),
                        (2, false, '2026-03-31 10:20:11', '2026-03-31 10:20:11.456', '2026-03-31 10:20:11.456789'),
                        (3, NULL, NULL, NULL, NULL)
                    """);

            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW COLUMNS FROM doris.test.type_mapping_extended"))
                        .contains(row("id", "bigint", "", ""))
                        .contains(row("bool_col", "boolean", "", ""))
                        .contains(row("created_at", "timestamp(0)", "", ""))
                        .contains(row("created_at_millis", "timestamp(3)", "", ""))
                        .contains(row("created_at_micros", "timestamp(6)", "", ""));
                assertThat(onTrino().executeQuery(
                        """
                        SELECT bool_col, CAST(created_at AS VARCHAR), CAST(created_at_millis AS VARCHAR), CAST(created_at_micros AS VARCHAR)
                        FROM doris.test.type_mapping_extended
                        ORDER BY id
                        """))
                        .containsOnly(
                                row(true, "2026-03-31 09:15:01", "2026-03-31 09:15:01.123", "2026-03-31 09:15:01.123456"),
                                row(false, "2026-03-31 10:20:11", "2026-03-31 10:20:11.456", "2026-03-31 10:20:11.456789"),
                                row(null, null, null, null));
            });
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS test.type_mapping_extended");
        }
    }

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testMixedCaseTableIsReadableViaLowercaseAlias()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS MixedCase_DB");
        onDoris().executeQuery("DROP TABLE IF EXISTS MixedCase_DB.OrderEvents_Mix");

        try {
            onDoris().executeQuery(
                    """
                    CREATE TABLE MixedCase_DB.OrderEvents_Mix (
                        event_id INT,
                        event_name VARCHAR(32)
                    )
                    DUPLICATE KEY(event_id)
                    DISTRIBUTED BY HASH(event_id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """);
            onDoris().executeQuery(
                    """
                    INSERT INTO MixedCase_DB.OrderEvents_Mix VALUES
                        (1, 'created'),
                        (2, 'shipped')
                    """);

            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM doris"))
                        .contains(row("mixedcase_db"));
                assertThat(onTrino().executeQuery("SHOW TABLES FROM doris.mixedcase_db"))
                        .contains(row("orderevents_mix"));
                assertThat(onTrino().executeQuery("SELECT event_id, event_name FROM doris.mixedcase_db.orderevents_mix ORDER BY event_id"))
                        .containsOnly(row(1, "created"), row(2, "shipped"));
            });
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS MixedCase_DB.OrderEvents_Mix");
            onDoris().executeQuery("DROP DATABASE IF EXISTS MixedCase_DB");
        }
    }
}
