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
package io.trino.verifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Table;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.verifier.QueryType.READ;
import static io.trino.verifier.VerifyCommand.statementToQueryType;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestShadowing
{
    private static final String CATALOG = "TEST_REWRITE";
    private static final String SCHEMA = "PUBLIC";
    private static final String URL = "jdbc:h2:mem:" + CATALOG;

    private final Handle handle;

    public TestShadowing()
    {
        handle = Jdbi.open(URL);
    }

    @AfterAll
    public void close()
    {
        handle.close();
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        handle.execute("CREATE TABLE \"my_test_table\" (column1 BIGINT, column2 DOUBLE)");
        SqlParser parser = new SqlParser();
        Query query = new Query(CATALOG, SCHEMA, ImmutableList.of(), "CREATE TABLE my_test_table AS SELECT 1 column1, CAST('2.0' AS DOUBLE) column2 LIMIT 1", ImmutableList.of(), null, null, ImmutableMap.of());
        QueryRewriter rewriter = new QueryRewriter(parser, URL, QualifiedName.of("tmp_"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 1, new Duration(10, SECONDS));
        Query rewrittenQuery = rewriter.shadowQuery(query);
        assertThat(rewrittenQuery.getPreQueries().size()).isEqualTo(1);
        assertThat(rewrittenQuery.getPostQueries().size()).isEqualTo(1);

        CreateTableAsSelect createTableAs = (CreateTableAsSelect) parser.createStatement(rewrittenQuery.getPreQueries().get(0));
        assertThat(createTableAs.getName().getParts().size()).isEqualTo(1);
        assertThat(createTableAs.getName().getSuffix().startsWith("tmp_")).isTrue();
        assertThat(createTableAs.getName().getSuffix().contains("my_test_table")).isFalse();

        assertThat(statementToQueryType(parser, rewrittenQuery.getQuery())).isEqualTo(READ);

        Table table = new Table(createTableAs.getName());
        SingleColumn column1 = new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new Identifier("COLUMN1"))));
        SingleColumn column2 = new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new FunctionCall(QualifiedName.of("round"), ImmutableList.of(new Identifier("COLUMN2"), new LongLiteral("1"))))));
        assertThat(parser.createStatement(rewrittenQuery.getQuery())).isEqualTo(simpleQuery(selectList(column1, column2), table));

        assertThat(parser.createStatement(rewrittenQuery.getPostQueries().get(0))).isEqualTo(new DropTable(new NodeLocation(1, 1), createTableAs.getName(), true));
    }

    @Test
    public void testCreateTableAsSelectDifferentCatalog()
            throws Exception
    {
        handle.execute("CREATE TABLE \"my_test_table2\" (column1 BIGINT, column2 DOUBLE)");
        SqlParser parser = new SqlParser();
        Query query = new Query(CATALOG, SCHEMA, ImmutableList.of(), "CREATE TABLE public.my_test_table2 AS SELECT 1 column1, 2E0 column2", ImmutableList.of(), null, null, ImmutableMap.of());
        QueryRewriter rewriter = new QueryRewriter(parser, URL, QualifiedName.of("other_catalog", "other_schema", "tmp_"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 1, new Duration(10, SECONDS));
        Query rewrittenQuery = rewriter.shadowQuery(query);
        assertThat(rewrittenQuery.getPreQueries().size()).isEqualTo(1);
        CreateTableAsSelect createTableAs = (CreateTableAsSelect) parser.createStatement(rewrittenQuery.getPreQueries().get(0));
        assertThat(createTableAs.getName().getParts().size()).isEqualTo(3);
        assertThat(createTableAs.getName().getPrefix().get()).isEqualTo(QualifiedName.of("other_catalog", "other_schema"));
        assertThat(createTableAs.getName().getSuffix().startsWith("tmp_")).isTrue();
        assertThat(createTableAs.getName().getSuffix().contains("my_test_table")).isFalse();
    }

    @Test
    public void testInsert()
            throws Exception
    {
        handle.execute("CREATE TABLE \"test_insert_table\" (a BIGINT, b DOUBLE, c VARCHAR)");
        SqlParser parser = new SqlParser();
        Query query = new Query(CATALOG, SCHEMA, ImmutableList.of(), "INSERT INTO test_insert_table (b, a, c) values (1.1, 1, 'a'), (2.0, 2, 'b'), (3.1, 3, 'c')", ImmutableList.of(), null, null, ImmutableMap.of());
        QueryRewriter rewriter = new QueryRewriter(parser, URL, QualifiedName.of("other_catalog", "other_schema", "tmp_"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 1, new Duration(10, SECONDS));
        Query rewrittenQuery = rewriter.shadowQuery(query);

        assertThat(rewrittenQuery.getPreQueries().size()).isEqualTo(2);
        CreateTable createTable = (CreateTable) parser.createStatement(rewrittenQuery.getPreQueries().get(0));
        assertThat(createTable.getName().getParts().size()).isEqualTo(3);
        assertThat(createTable.getName().getPrefix().get()).isEqualTo(QualifiedName.of("other_catalog", "other_schema"));
        assertThat(createTable.getName().getSuffix().startsWith("tmp_")).isTrue();
        assertThat(createTable.getName().getSuffix().contains("test_insert_table")).isFalse();

        Insert insert = (Insert) parser.createStatement(rewrittenQuery.getPreQueries().get(1));
        assertThat(insert.getTarget()).isEqualTo(createTable.getName());
        assertThat(insert.getColumns()).isEqualTo(Optional.of(ImmutableList.of(identifier("b"), identifier("a"), identifier("c"))));

        Table table = new Table(createTable.getName());
        SingleColumn columnA = new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new Identifier("A"))));
        SingleColumn columnB = new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new FunctionCall(QualifiedName.of("round"), ImmutableList.of(new Identifier("B"), new LongLiteral("1"))))));
        SingleColumn columnC = new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(new Identifier("C"))));
        assertThat(parser.createStatement(rewrittenQuery.getQuery())).isEqualTo(simpleQuery(selectList(columnA, columnB, columnC), table));

        assertThat(rewrittenQuery.getPostQueries().size()).isEqualTo(1);
        assertThat(parser.createStatement(rewrittenQuery.getPostQueries().get(0))).isEqualTo(new DropTable(new NodeLocation(1, 1), createTable.getName(), true));
    }
}
