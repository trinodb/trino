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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.tree.SaveMode.FAIL;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlFormatter
{
    @Test
    public void testShowCatalogs()
    {
        assertThat(formatSql(
                new ShowCatalogs(Optional.empty(), Optional.empty())))
                .isEqualTo("SHOW CATALOGS");
        assertThat(formatSql(
                new ShowCatalogs(Optional.of("%"), Optional.empty())))
                .isEqualTo("SHOW CATALOGS LIKE '%'");
        assertThat(formatSql(
                new ShowCatalogs(Optional.of("%$_%"), Optional.of("$"))))
                .isEqualTo("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'");
        assertThat(formatSql(
                new ShowCatalogs(Optional.of("%機動隊"), Optional.of("😂"))))
                .isEqualTo("SHOW CATALOGS LIKE '%機動隊' ESCAPE '😂'");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(formatSql(
                new ShowSchemas(Optional.empty(), Optional.empty(), Optional.empty())))
                .isEqualTo("SHOW SCHEMAS");
        assertThat(formatSql(
                new ShowSchemas(Optional.empty(), Optional.of("%"), Optional.empty())))
                .isEqualTo("SHOW SCHEMAS LIKE '%'");
        assertThat(formatSql(
                new ShowSchemas(Optional.empty(), Optional.of("%$_%"), Optional.of("$"))))
                .isEqualTo("SHOW SCHEMAS LIKE '%$_%' ESCAPE '$'");
        assertThat(formatSql(
                new ShowSchemas(Optional.empty(), Optional.of("%機動隊"), Optional.of("😂"))))
                .isEqualTo("SHOW SCHEMAS LIKE '%機動隊' ESCAPE '😂'");
    }

    @Test
    public void testShowTables()
    {
        assertThat(formatSql(
                new ShowTables(Optional.empty(), Optional.empty(), Optional.empty())))
                .isEqualTo("SHOW TABLES");
        assertThat(formatSql(
                new ShowTables(Optional.empty(), Optional.of("%"), Optional.empty())))
                .isEqualTo("SHOW TABLES LIKE '%'");
        assertThat(formatSql(
                new ShowTables(Optional.empty(), Optional.of("%$_%"), Optional.of("$"))))
                .isEqualTo("SHOW TABLES LIKE '%$_%' ESCAPE '$'");
        assertThat(formatSql(
                new ShowTables(Optional.empty(), Optional.of("%機動隊"), Optional.of("😂"))))
                .isEqualTo("SHOW TABLES LIKE '%機動隊' ESCAPE '😂'");
    }

    @Test
    public void testShowColumns()
    {
        assertThat(formatSql(
                new ShowColumns(QualifiedName.of("a"), Optional.empty(), Optional.empty())))
                .isEqualTo("SHOW COLUMNS FROM a");
        assertThat(formatSql(
                new ShowColumns(QualifiedName.of("a"), Optional.of("%"), Optional.empty())))
                .isEqualTo("SHOW COLUMNS FROM a LIKE '%'");
        assertThat(formatSql(
                new ShowColumns(QualifiedName.of("a"), Optional.of("%$_%"), Optional.of("$"))))
                .isEqualTo("SHOW COLUMNS FROM a LIKE '%$_%' ESCAPE '$'");
        assertThat(formatSql(
                new ShowColumns(QualifiedName.of("a"), Optional.of("%機動隊"), Optional.of("😂"))))
                .isEqualTo("SHOW COLUMNS FROM a LIKE '%機動隊' ESCAPE '😂'");
    }

    @Test
    public void testShowFunctions()
    {
        assertThat(formatSql(
                new ShowFunctions(Optional.empty(), Optional.empty())))
                .isEqualTo("SHOW FUNCTIONS");
        assertThat(formatSql(
                new ShowFunctions(Optional.of("%"), Optional.empty())))
                .isEqualTo("SHOW FUNCTIONS LIKE '%'");
        assertThat(formatSql(
                new ShowFunctions(Optional.of("%$_%"), Optional.of("$"))))
                .isEqualTo("SHOW FUNCTIONS LIKE '%$_%' ESCAPE '$'");
        assertThat(formatSql(
                new ShowFunctions(Optional.of("%機動隊"), Optional.of("😂"))))
                .isEqualTo("SHOW FUNCTIONS LIKE '%機動隊' ESCAPE '😂'");
    }

    @Test
    public void testShowSession()
    {
        assertThat(formatSql(
                new ShowSession(Optional.empty(), Optional.empty())))
                .isEqualTo("SHOW SESSION");
        assertThat(formatSql(
                new ShowSession(Optional.of("%"), Optional.empty())))
                .isEqualTo("SHOW SESSION LIKE '%'");
        assertThat(formatSql(
                new ShowSession(Optional.of("%$_%"), Optional.of("$"))))
                .isEqualTo("SHOW SESSION LIKE '%$_%' ESCAPE '$'");
        assertThat(formatSql(
                new ShowSession(Optional.of("%機動隊"), Optional.of("😂"))))
                .isEqualTo("SHOW SESSION LIKE '%機動隊' ESCAPE '😂'");
    }

    @Test
    public void testIdentifiers()
    {
        // Reserved keyword
        assertThat(formatSql(new Identifier("exists", false))).isEqualTo("\"exists\"");
        assertThat(formatSql(new Identifier("exists", true))).isEqualTo("\"exists\"");
        assertThat(formatSql(new Identifier("\"exists\"", true))).isEqualTo("\"\"\"exists\"\"\"");

        // Non-reserved keyword
        assertThat(formatSql(new Identifier("analyze", false))).isEqualTo("analyze");
        assertThat(formatSql(new Identifier("analyze", true))).isEqualTo("\"analyze\"");
        assertThat(formatSql(new Identifier("\"analyze\"", true))).isEqualTo("\"\"\"analyze\"\"\"");

        // ANSI-compliant identifier
        assertThat(formatSql(new Identifier("account", false))).isEqualTo("account");
        assertThat(formatSql(new Identifier("account", true))).isEqualTo("\"account\"");
        assertThat(formatSql(new Identifier("\"account\"", true))).isEqualTo("\"\"\"account\"\"\"");

        // Non-ANSI compliant identifier
        assertThat(formatSql(new Identifier("1", true))).isEqualTo("\"1\"");
        assertThat(formatSql(new Identifier("\"1\"", true))).isEqualTo("\"\"\"1\"\"\"");
    }

    @Test
    public void testCreateCatalog()
    {
        assertThat(formatSql(
                new CreateCatalog(
                        new Identifier("test"),
                        false,
                        new Identifier("conn"),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty())))
                .isEqualTo("CREATE CATALOG test USING conn");
        assertThat(formatSql(
                new CreateCatalog(
                        new Identifier("test"),
                        false,
                        new Identifier("conn"),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.of("test comment"))))
                .isEqualTo("CREATE CATALOG test USING conn\n" +
                        "COMMENT 'test comment'");
        assertThat(formatSql(
                new CreateCatalog(
                        new Identifier("test"),
                        false,
                        new Identifier("conn"),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.of("攻殻機動隊"))))
                .isEqualTo("CREATE CATALOG test USING conn\n" +
                        "COMMENT '攻殻機動隊'");
    }

    @Test
    public void testCreateTable()
    {
        BiFunction<String, String, CreateTable> createTable = (tableName, columnName) -> {
            NodeLocation location = new NodeLocation(1, 1);
            Identifier type = new Identifier(location, "VARCHAR", false);
            return new CreateTable(
                    QualifiedName.of(ImmutableList.of(new Identifier(tableName, false))),
                    ImmutableList.of(new ColumnDefinition(
                            QualifiedName.of(columnName),
                            new GenericDataType(location, type, ImmutableList.of()),
                            true,
                            ImmutableList.of(),
                            Optional.empty())),
                    FAIL,
                    ImmutableList.of(),
                    Optional.empty());
        };
        String createTableSql = "CREATE TABLE %s (\n   %s VARCHAR\n)";

        assertThat(formatSql(createTable.apply("table_name", "column_name")))
                .isEqualTo(String.format(createTableSql, "table_name", "column_name"));
        assertThat(formatSql(createTable.apply("exists", "exists")))
                .isEqualTo(String.format(createTableSql, "\"exists\"", "\"exists\""));

        // Create a table with table comment
        assertThat(formatSql(
                new CreateTable(
                        QualifiedName.of(ImmutableList.of(new Identifier("test", false))),
                        ImmutableList.of(new ColumnDefinition(
                                QualifiedName.of("col"),
                                new GenericDataType(new NodeLocation(1, 1), new Identifier("VARCHAR", false), ImmutableList.of()),
                                true,
                                ImmutableList.of(),
                                Optional.empty())),
                        FAIL,
                        ImmutableList.of(),
                        Optional.of("攻殻機動隊"))))
                .isEqualTo("CREATE TABLE test (\n" +
                        "   col VARCHAR\n" +
                        ")\n" +
                        "COMMENT '攻殻機動隊'");

        // Create a table with column comment
        assertThat(formatSql(
                new CreateTable(
                        QualifiedName.of(ImmutableList.of(new Identifier("test", false))),
                        ImmutableList.of(new ColumnDefinition(
                                QualifiedName.of("col"),
                                new GenericDataType(new NodeLocation(1, 1), new Identifier("VARCHAR", false), ImmutableList.of()),
                                true,
                                ImmutableList.of(),
                                Optional.of("攻殻機動隊"))),
                        FAIL,
                        ImmutableList.of(),
                        Optional.empty())))
                .isEqualTo("CREATE TABLE test (\n" +
                        "   col VARCHAR COMMENT '攻殻機動隊'\n" +
                        ")");
    }

    @Test
    public void testCreateTableAsSelect()
    {
        BiFunction<String, String, CreateTableAsSelect> createTableAsSelect = (tableName, columnName) -> {
            Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));
            return new CreateTableAsSelect(
                    QualifiedName.of(ImmutableList.of(new Identifier(tableName, false))),
                    query,
                    FAIL,
                    ImmutableList.of(),
                    true,
                    Optional.of(ImmutableList.of(new Identifier(columnName, false))),
                    Optional.empty());
        };
        String createTableSql = "CREATE TABLE %s( %s ) AS SELECT *\nFROM\n  t\n";

        assertThat(formatSql(createTableAsSelect.apply("table_name", "column_name")))
                .isEqualTo(String.format(createTableSql, "table_name", "column_name"));
        assertThat(formatSql(createTableAsSelect.apply("exists", "exists")))
                .isEqualTo(String.format(createTableSql, "\"exists\"", "\"exists\""));

        assertThat(formatSql(
                new CreateTableAsSelect(
                        QualifiedName.of(ImmutableList.of(new Identifier("test", false))),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        FAIL,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(new Identifier("col", false))),
                        Optional.of("攻殻機動隊"))))
                .isEqualTo("CREATE TABLE test( col )\n" +
                        "COMMENT '攻殻機動隊' AS SELECT *\n" +
                        "FROM\n" +
                        "  t\n");
    }

    @Test
    public void testCreateView()
    {
        assertThat(formatSql(
                new CreateView(
                        new NodeLocation(1, 1),
                        QualifiedName.of("test"),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        false,
                        Optional.empty(),
                        Optional.empty())))
                .isEqualTo("CREATE VIEW test AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  t\n");
        assertThat(formatSql(
                new CreateView(
                        new NodeLocation(1, 1),
                        QualifiedName.of("test"),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        false,
                        Optional.of("攻殻機動隊"),
                        Optional.empty())))
                .isEqualTo("CREATE VIEW test COMMENT '攻殻機動隊' AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  t\n");
    }

    @Test
    public void testCreateMaterializedView()
    {
        assertThat(formatSql(
                new CreateMaterializedView(
                        Optional.empty(),
                        QualifiedName.of("test_mv"),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("test_base"))),
                        false,
                        false,
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty())))
                .isEqualTo("CREATE MATERIALIZED VIEW test_mv AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  test_base\n");
        assertThat(formatSql(
                new CreateMaterializedView(
                        Optional.empty(),
                        QualifiedName.of("test_mv"),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("test_base"))),
                        false,
                        false,
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.of("攻殻機動隊"))))
                .isEqualTo("CREATE MATERIALIZED VIEW test_mv\n" +
                        "COMMENT '攻殻機動隊' AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  test_base\n");
    }

    @Test
    public void testAddColumn()
    {
        assertThat(formatSql(
                new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("c"),
                                new GenericDataType(new NodeLocation(1, 1), new Identifier("VARCHAR", false), ImmutableList.of()),
                                true,
                                emptyList(),
                                Optional.empty()),
                        false, false)))
                .isEqualTo("ALTER TABLE foo.t ADD COLUMN c VARCHAR");
        assertThat(formatSql(
                new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("c"),
                                new GenericDataType(new NodeLocation(1, 1), new Identifier("VARCHAR", false), ImmutableList.of()),
                                true,
                                emptyList(),
                                Optional.of("攻殻機動隊")),
                        false, false)))
                .isEqualTo("ALTER TABLE foo.t ADD COLUMN c VARCHAR COMMENT '攻殻機動隊'");
    }

    @Test
    public void testCommentOnTable()
    {
        assertThat(formatSql(
                new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.of("test"))))
                .isEqualTo("COMMENT ON TABLE a IS 'test'");
        assertThat(formatSql(
                new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.of("攻殻機動隊"))))
                .isEqualTo("COMMENT ON TABLE a IS '攻殻機動隊'");
    }

    @Test
    public void testCommentOnView()
    {
        assertThat(formatSql(
                new Comment(Comment.Type.VIEW, QualifiedName.of("a"), Optional.of("test"))))
                .isEqualTo("COMMENT ON VIEW a IS 'test'");
        assertThat(formatSql(
                new Comment(Comment.Type.VIEW, QualifiedName.of("a"), Optional.of("攻殻機動隊"))))
                .isEqualTo("COMMENT ON VIEW a IS '攻殻機動隊'");
    }

    @Test
    public void testCommentOnColumn()
    {
        assertThat(formatSql(
                new Comment(Comment.Type.COLUMN, QualifiedName.of("test", "a"), Optional.of("test"))))
                .isEqualTo("COMMENT ON COLUMN test.a IS 'test'");
        assertThat(formatSql(
                new Comment(Comment.Type.COLUMN, QualifiedName.of("test", "a"), Optional.of("攻殻機動隊"))))
                .isEqualTo("COMMENT ON COLUMN test.a IS '攻殻機動隊'");
    }

    @Test
    public void testExecuteImmediate()
    {
        assertThat(formatSql(
                new ExecuteImmediate(
                        new NodeLocation(1, 1),
                        new StringLiteral(new NodeLocation(1, 19), "SELECT * FROM foo WHERE col1 = ? AND col2 = ?"),
                        ImmutableList.of(new LongLiteral("42"), new StringLiteral("bar")))))
                .isEqualTo("EXECUTE IMMEDIATE\n'SELECT * FROM foo WHERE col1 = ? AND col2 = ?'\nUSING 42, 'bar'");
        assertThat(formatSql(
                new ExecuteImmediate(
                        new NodeLocation(1, 1),
                        new StringLiteral(new NodeLocation(1, 19), "SELECT * FROM foo WHERE col1 = 'bar'"),
                        ImmutableList.of())))
                .isEqualTo("EXECUTE IMMEDIATE\n'SELECT * FROM foo WHERE col1 = ''bar'''");
        assertThat(formatSql(
                new ExecuteImmediate(
                        new NodeLocation(1, 1),
                        new StringLiteral(new NodeLocation(1, 19), "SELECT * FROM foo WHERE col1 = '攻殻機動隊'"),
                        ImmutableList.of())))
                .isEqualTo("EXECUTE IMMEDIATE\n" +
                        "'SELECT * FROM foo WHERE col1 = ''攻殻機動隊'''");
    }
}
