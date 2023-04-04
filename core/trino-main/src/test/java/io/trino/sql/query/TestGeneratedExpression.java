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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.GeneratedExpression;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGeneratedExpression
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
                        return columnMetadata("101");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_multiple_column_constraint"))) {
                        return ImmutableList.<ColumnMetadata>builder()
                                .add(ColumnMetadata.builder().setName("nationkey").setType(BIGINT).setGeneratedColumn(Optional.of(new GeneratedExpression("1"))).build())
                                .add(ColumnMetadata.builder().setName("name").setType(VARCHAR).setGeneratedColumn(Optional.of(new GeneratedExpression("'test generated text'"))).build())
                                .add(new ColumnMetadata("regionkey", BIGINT))
                                .add(new ColumnMetadata("comment", VARCHAR))
                                .build();
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_invalid_function"))) {
                        return columnMetadata("invalid_function(regionkey)");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_subquery"))) {
                        return columnMetadata("nationkey > (SELECT count(*) FROM nation)");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_date"))) {
                        return columnMetadata("CURRENT_DATE");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_time"))) {
                        return columnMetadata("CURRENT_TIME");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_current_timestamp"))) {
                        return columnMetadata("CURRENT_TIMESTAMP");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_localtime"))) {
                        return columnMetadata("LOCALTIME");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_localtimestamp"))) {
                        return columnMetadata("LOCALTIMESTAMP");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_not_deterministic"))) {
                        return columnMetadata("random()");
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

    private static List<ColumnMetadata> columnMetadata(String expression)
    {
        return ImmutableList.<ColumnMetadata>builder()
                .add(ColumnMetadata.builder().setName("nationkey").setType(BIGINT).setGeneratedColumn(Optional.of(new GeneratedExpression(expression))).build())
                .add(new ColumnMetadata("name", VARCHAR))
                .add(new ColumnMetadata("regionkey", BIGINT))
                .add(new ColumnMetadata("comment", VARCHAR))
                .build();
    }

    /**
     * @see #testMergeInsert()
     */
    @Test
    public void testInsert()
    {
        assertThat(assertions.query("INSERT INTO mock.tiny.nation (name, regionkey, comment) VALUES ('POLAND', 0, 'No comment')"))
                .matches("SELECT BIGINT '1'");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES (102, 'POLAND', 11, 'No comment')"))
                .hasMessage("Cannot specify a value for a column with GENERATED ALWAYS: nationkey");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation (nationkey) VALUES (102)"))
                .hasMessage("Cannot specify a value for a column with GENERATED ALWAYS: nationkey");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation (nationkey, name) VALUES (102, 'POLAND')"))
                .hasMessage("Cannot specify a value for a column with GENERATED ALWAYS: nationkey");
    }

    /**
     * Like {@link #testInsert} but using the MERGE statement.
     */
    @Test
    public void testMergeInsert()
    {
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (26, 'POLAND', 0, 'No comment')
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES (26, 'POLAND', 0, 'No comment'), (27, 'HOLLAND', 0, 'A comment')) t(a,b,c,d) ON nationkey = a
                WHEN NOT MATCHED THEN INSERT VALUES (a,b,c,d)
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");

        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (NULL)
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (0)
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
    }

    @Test
    public void testInsertGeneratedMultipleColumns()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_multiple_column_constraint (nationkey, comment) VALUES (101, 'No comment')"))
                .hasMessage("Cannot specify a value for a column with GENERATED ALWAYS: nationkey");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_multiple_column_constraint (name, comment) VALUES ('POLAND', 'No comment')"))
                .hasMessage("Cannot specify a value for a column with GENERATED ALWAYS: name");

        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_multiple_column_constraint (nationkey, name) VALUES (101, 'POLAND')"))
                .hasMessage("Cannot specify a value for a column with GENERATED ALWAYS: nationkey");
    }

    @Test
    public void testInsertSubquery()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_subquery (name, regionkey, comment) VALUES ('POLAND', 0, 'No comment')"))
                .hasMessage("line 1:11: Generated expression should not contain subquery: nationkey");
    }

    @Test
    public void testInsertUnsupportedCurrentDate()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_current_date (name, regionkey, comment) VALUES ('POLAND', 0, 'No comment')"))
                .hasMessage("line 1:1: Generated expression should not contain temporal expression: nationkey");
    }

    @Test
    public void testInsertUnsupportedCurrentTime()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_current_time (name, regionkey, comment) VALUES ('POLAND', 0, 'No comment')"))
                .hasMessage("line 1:1: Generated expression should not contain temporal expression: nationkey");
    }

    @Test
    public void testInsertUnsupportedCurrentTimestamp()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_current_timestamp VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessage("line 1:1: Generated expression should not contain temporal expression: nationkey");
    }

    @Test
    public void testInsertUnsupportedLocaltime()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_localtime (name, regionkey, comment) VALUES ('POLAND', 0, 'No comment')"))
                .hasMessage("line 1:1: Generated expression should not contain temporal expression: nationkey");
    }

    @Test
    public void testInsertUnsupportedLocaltimestamp()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_localtimestamp VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessage("line 1:1: Generated expression should not contain temporal expression: nationkey");
    }

    @Test
    public void testInsertUnsupportedGeneratedExpression()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_invalid_function (name, regionkey, comment) VALUES ('POLAND', 0, 'No comment')"))
                .hasMessageContaining("Function 'invalid_function' not registered");
    }

    @Test
    public void testInsertNotDeterministic()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_not_deterministic VALUES (100, 'POLAND', 0, 'No comment')"))
                .hasMessage("line 1:1: Generated expression should be deterministic: nationkey");
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
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5) t(x) ON regionkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 1,11) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 11,12,13,14,15) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
    }

    /**
     * @see #testMergeUpdate()
     */
    @Test
    public void testUpdate()
    {
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a generated column is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 2, 3)"))
                .hasMessage("line 1:1: Updating a table with a generated column is not supported");
    }

    /**
     * Like {@link #testUpdate()} but using the MERGE statement.
     */
    @Test
    public void testMergeUpdate()
    {
        assertThatThrownBy(() -> assertions.query("""
                MERGE INTO mock.tiny.nation USING (VALUES 5) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .hasMessage("line 1:1: Cannot merge into a table with generated columns");
    }
}
