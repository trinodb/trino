package io.hivesql.sql.parser;

import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import org.testng.annotations.Test;

import java.util.Optional;

public class LikeRlike extends SQLTester {

    @Test
    public void testRlikeExpression()
    {
        String sql = "SELECT x from m where x rlike 'hajksda'";

        checkASTNode(sql);
    }
    @Test
    public void testLikeExpression()
    {
        String sql = "SELECT x from m where x like 'hajksda'";

        checkASTNode(sql);
    }
    @Test
    public void testLikeSeDe()
    {
        String sql = "x like 'sdsada'";

        Node node = new SqlParser().createExpression(sql);

        String fomatedAst = ExpressionFormatter.formatExpression((Expression) node);
        Node node1 = new SqlParser().createExpression(fomatedAst);
        checkASTNode(node, node1);

    }
    @Test
    public void testRlikeSeDe()
    {
        String sql = "x RLIKE 'sdsada'";

        Node node = new SqlParser().createExpression(sql);

        String fomatedAst = ExpressionFormatter.formatExpression((Expression) node);
        Node node1 = new SqlParser().createExpression(fomatedAst);
        checkASTNode(node, node1);

    }

}
