package io.hivesql.sql.parser;

import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import org.testng.annotations.Test;

import java.util.Optional;

public class ArithmeticBinary extends SQLTester {

    @Test
    public void testBase()
    {
        String sql = "SELECT x|y&z^m";

        checkASTNode(sql);
    }
    @Test
    public void testFromTable()
    {
        String sql = "SELECT x&y|z^m from m where x like 'hajksda'";

        checkASTNode(sql);
    }

    @Test
    public void testSeDe()
    {
        String sql = "x&y|z^m+c-d/6%5 DIV m";

        Node node = new SqlParser().createExpression(sql);

        String fomatedAst = ExpressionFormatter.formatExpression((Expression) node);
        Node node1 = new SqlParser().createExpression(fomatedAst);
        checkASTNode(node, node1);

    }

}
