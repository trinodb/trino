package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class WindowFunction extends SQLTester {

    @Test
    public void testPartitionBy()
    {
        String sql = "SELECT a, COUNT(b) OVER (PARTITION BY c) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByMultipleCols()
    {
        String sql = "SELECT a, COUNT(b) OVER (PARTITION BY c, d) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderBy()
    {
        String sql = "SELECT a, COUNT(b) OVER (PARTITION BY c ORDER BY d) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByMultipleCols()
    {
        String sql = "SELECT a, COUNT(b) OVER (PARTITION BY c, d ORDER BY e, f) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRowUnBoundedWindowFramePreceding()
    {
        String sql = "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRowUnBoundedWindowFrameFollowing()
    {
        String sql = "SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRowBoundedWindowFrame()
    {
        String sql = "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRowBoundedWindowFrameOnBothSide()
    {
        String sql = "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRangeUnBoundedWindowFramePreceding()
    {
        String sql = "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM T";

        checkASTNode(sql);
    }


    @Test
    public void testPartitionByOrderByRangeUnBoundedWindowFrameFollowing()
    {
        String sql = "SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRangeBoundedWindowFrame()
    {
        String sql = "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testPartitionByOrderByRangeBoundedWindowFrameOnBothSide()
    {
        String sql = "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testMultipleOverClauses()
    {
        String sql = "" +
                "SELECT \n" +
                " a,\n" +
                " COUNT(b) OVER (PARTITION BY c) AS b_count,\n" +
                " SUM(b) OVER (PARTITION BY c) b_sum\n" +
                "FROM T"
                ;

        checkASTNode(sql);
    }

    @Test
    public void testLeadFunction()
    {
        String sql = "SELECT a, LEAD(a) OVER (PARTITION BY b ORDER BY C) FROM T";

        checkASTNode(sql);
    }

    @Test
    public void testCountDistinct()
    {
        String sql = "SELECT a, COUNT(distinct a) OVER (PARTITION BY b) FROM T";

        checkASTNode(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void runWindowClauseShouldThrowException()
    {
        String hiveSql = "" +
                "SELECT a, SUM(b) OVER w\n" +
                "FROM T\n" +
                "WINDOW w AS (PARTITION BY c ORDER BY d ROWS UNBOUNDED PRECEDING)";

        runHiveSQL(hiveSql);
    }
}
