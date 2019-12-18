import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;

public class JoinReportTester
{

    /*
    * Note: after editing Sqlbase.g4, run maven clean install
    * */

    public static void main(String[] args)
    {
        SqlParser sqlParser = new SqlParser(  );

        final String appId = "app_id";
        final String sumImp = "sum(imp)";
        final String caseStatement = "CASE WHEN platform = 'ios' THEN itunes_id WHEN platform = 'android' THEN app_package_name ELSE '' END";
        final String hour = "TO_CHAR(event_day, 'HH:00')";
        final String date = "DATE(event_day)";
        final String safeDivide = "SUM(ad_fetch_size) / NULLIFZERO ( SUM(srv) )";

        Expression a = sqlParser.createExpression( appId );
        Expression b = sqlParser.createExpression( sumImp );
        Expression c = sqlParser.createExpression( caseStatement );
        Expression d = sqlParser.createExpression( hour );
        Expression e = sqlParser.createExpression( date );
        Expression f = sqlParser.createExpression( safeDivide );

        System.out.println(  );



//        //
//        // Expressions that should fail
//        //
//        final String sumImpWithAlias = "sum(r.imp) as sum_imp";
//        final String twoCols = "app_id, campaign_id";
//        sqlParser.createExpression( sumImpWithAlias );
//        sqlParser.createExpression( twoCols );
    }

    public static void identifierTest()
    {
        SqlParser sqlParser = new SqlParser(  );

        final String appId = "app_id";

        Expression a = sqlParser.createExpression( appId );

        System.out.println( "Old Expr: " + ExpressionFormatter.formatExpression( a ) );

        ( (Identifier) a ).val

        System.out.println( "New Expr: " + ExpressionFormatter.formatExpression( a ) );
    }
}
