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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCheckConstraint
{
    private static final String LOCAL_CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String USER = "user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(LOCAL_CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION).build();

        runner.createCatalog(LOCAL_CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        MockConnectorFactory mock = MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_multiple_column_constraint"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_invalid_function"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_not_boolean_expression"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_subquery"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_date"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_time"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_timestamp"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_localtime"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_localtimestamp"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_not_deterministic"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withCheckConstraints(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return ImmutableList.of("regionkey < 10");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_multiple_column_constraint"))) {
                        return ImmutableList.of("nationkey > 100 AND regionkey > 50");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_invalid_function"))) {
                        return ImmutableList.of("invalid_function(nationkey) > 100");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_not_boolean_expression"))) {
                        return ImmutableList.of("1 + 1");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_subquery"))) {
                        return ImmutableList.of("nationkey > (SELECT count(*) FROM nation)");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_date"))) {
                        return ImmutableList.of("CURRENT_DATE > DATE '2022-12-31'");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_time"))) {
                        return ImmutableList.of("CURRENT_TIME > TIME '12:34:56.123+00:00'");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_timestamp"))) {
                        return ImmutableList.of("CURRENT_TIMESTAMP > TIMESTAMP '2022-12-31 23:59:59'");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_localtime"))) {
                        return ImmutableList.of("LOCALTIME > TIME '12:34:56.123'");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_localtimestamp"))) {
                        return ImmutableList.of("LOCALTIMESTAMP > TIMESTAMP '2022-12-31 23:59:59'");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_not_deterministic"))) {
                        return ImmutableList.of("nationkey > random()");
                    }
                    throw new UnsupportedOperationException();
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_DATA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_multiple_column_constraint"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .build();

        runner.createCatalog(MOCK_CATALOG, mock, ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    /**
     * @see #testMergeInsert()
     */
    @Test
    public void testInsert()
    {
        assertThat(assertions.query("INSERT INTO mock.tiny.nation VALUES (101, 'POLAND', 0, 'No comment')"))
                .matches("SELECT BIGINT '1'");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES (26, 'POLAND', 11, 'No comment')"))
                .hasMessage("Check constraint violation: (regionkey < 10)");
        assertThatThrownBy(() -> assertions.query("""
                INSERT INTO mock.tiny.nation VALUES
                (26, 'POLAND', 11, 'No comment'),
                (27, 'HOLLAND', 11, 'A comment')
                """))
                .hasMessage("Check constraint violation: (regionkey < 10)");
        assertThatThrownBy(() -> assertions.query("""
                INSERT INTO mock.tiny.nation VALUES
                (26, 'POLAND', 11, 'No comment'),
                (27, 'HOLLAND', 11, 'A comment')
                """))
                .hasMessage("Check constraint violation: (regionkey < 10)");
    }

    /**
     * Like {@link #testInsert} but using the MERGE statement.
     */
    @Test
    public void testMergeInsert()
    {
        // Within allowed check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (101, 'POLAND', 0, 'No comment')
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");

        // Outside allowed check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (26, 'POLAND', 0, 'No comment')
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES (26, 'POLAND', 0, 'No comment'), (27, 'HOLLAND', 0, 'A comment')) t(a,b,c,d) ON nationkey = a
                WHEN NOT MATCHED THEN INSERT VALUES (a,b,c,d)
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");

        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (NULL)
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (0)
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
    }

    @Test
    public void testInsertAllowUnknown()
    {
        // Predicate evaluates to UNKNOWN (e.g. NULL > 100) should not violate check constraint
        assertThat(assertions.query("INSERT INTO mock.tiny.nation(nationkey) VALUES (null)"))
                .matches("SELECT BIGINT '1'");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation(regionkey) VALUES (0)"))
                .matches("SELECT BIGINT '1'");
    }

    @Test
    public void testInsertCheckMultipleColumns()
    {
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_multiple_column_constraint VALUES (101, 'POLAND', 51, 'No comment')"))
                .matches("SELECT BIGINT '1'");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_multiple_column_constraint VALUES (101, 'POLAND', 50, 'No comment')"))
                .hasMessage("Check constraint violation: ((nationkey > 100) AND (regionkey > 50))");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_multiple_column_constraint VALUES (100, 'POLAND', 51, 'No comment')"))
                .hasMessage("Check constraint violation: ((nationkey > 100) AND (regionkey > 50))");
    }

    @Test
    public void testInsertSubquery()
    {
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_subquery VALUES (26, 'POLAND', 51, 'No comment')"))
                .matches("SELECT BIGINT '1'");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_subquery VALUES (10, 'POLAND', 0, 'No comment')"))
                .hasMessage("Check constraint violation: (nationkey > (SELECT count(*)\nFROM\n  nation\n))");
    }

    @Test
    public void testInsertUnsupportedCurrentDate()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_current_date VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Check constraint expression should not contain temporal expression");
    }

    @Test
    public void testInsertUnsupportedCurrentTime()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_current_time VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Check constraint expression should not contain temporal expression");
    }

    @Test
    public void testInsertUnsupportedCurrentTimestamp()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_current_timestamp VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Check constraint expression should not contain temporal expression");
    }

    @Test
    public void testInsertUnsupportedLocaltime()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_localtime VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Check constraint expression should not contain temporal expression");
    }

    @Test
    public void testInsertUnsupportedLocaltimestamp()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_localtimestamp VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Check constraint expression should not contain temporal expression");
    }

    @Test
    public void testInsertUnsupportedConstraint()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_invalid_function VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Function 'invalid_function' not registered");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_not_boolean_expression VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("to be of type BOOLEAN, but was integer");
    }

    @Test
    public void testInsertNotDeterministic()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_not_deterministic VALUES (100, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Check constraint expression should be deterministic");
    }

    /**
     * @see #testMergeDelete()
     */
    @Test
    public void testDelete()
    {
        assertThat(assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey < 3"))
                .matches("SELECT BIGINT '3'");
        assertThat(assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey IN (1, 2, 3)"))
                .matches("SELECT BIGINT '3'");
    }

    /**
     * Like {@link #testDelete()} but using the MERGE statement.
     */
    @Test
    public void testMergeDelete()
    {
        // Within allowed check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,2) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");

        // Outside allowed check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5) t(x) ON regionkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,11) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 11,12,13,14,15) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
    }

    /**
     * @see #testMergeUpdate()
     */
    @Test
    public void testUpdate()
    {
        // Within allowed check constraint
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 2, 3)"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");

        // Outside allowed check constraint
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 11)"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");

        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey = 11"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");

        // Within allowed check constraint, but updated rows are outside the check constraint
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");

        // Outside allowed check constraint, and updated rows are outside the check constraint
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey = 10"))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey = null "))
                .hasMessage("line 1:1: Updating a table with a check constraint is not supported");
    }

    /**
     * Like {@link #testUpdate()} but using the MERGE statement.
     */
    @Test
    public void testMergeUpdate()
    {
        // Within allowed check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 5) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");

        // Outside allowed check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5,6) t(x) ON regionkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1, 11) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 11) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");

        // Within allowed check constraint, but updated rows are outside the check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = 10
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = NULL
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");

        // Outside allowed check constraint, but updated rows are outside the check constraint
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = 13
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = NULL
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey IS NULL
                WHEN MATCHED THEN UPDATE SET nationkey = 13
                """))
                .hasMessage("line 1:1: Cannot merge into a table with check constraints");
    }
}
