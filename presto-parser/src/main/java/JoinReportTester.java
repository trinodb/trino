import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Node;

import java.util.List;

public class JoinReportTester
{

    /*
    * Note: after editing Sqlbase.g4, run maven clean install
    * */

    public static void main(String[] args)
    {
        SqlParser sqlParser = new SqlParser(  );

        final String appId = "app_id";
        final String rDotAppId = "r.app_id";
        final String sumImp = "sum(imp)";
        final String caseStatement = "CASE WHEN platform = 'ios' THEN itunes_id WHEN platform = 'android' THEN app_package_name ELSE '' END";
        final String hour = "TO_CHAR(event_day, 'HH:00')";
        final String date = "DATE(event_day)";
        final String safeDivide = "SUM(ad_fetch_size) / NULLIFZERO ( SUM(srv) )";



        Expression appIdExpr = sqlParser.createExpression( appId );
        Expression appidWithAliasExpr = sqlParser.createExpression( rDotAppId );
        Expression sumImpExpr = sqlParser.createExpression( sumImp );
        Expression caseStatementExpr = sqlParser.createExpression( caseStatement );
        Expression hourExpr = sqlParser.createExpression( hour );
        Expression dateExpr = sqlParser.createExpression( date );
        Expression safeDivideExpr = sqlParser.createExpression( safeDivide );

        AliasAdditionExpressionFormatter aliasAdder = new AliasAdditionExpressionFormatter( "r" );

        System.out.println( aliasAdder.process( appIdExpr ) );
        System.out.println(  aliasAdder.process( sumImpExpr ) );
        System.out.println(  aliasAdder.process( caseStatementExpr ) );
        System.out.println(  aliasAdder.process( hourExpr ) );
        System.out.println(  aliasAdder.process( dateExpr ) );
        System.out.println(  aliasAdder.process( safeDivideExpr ) );

        try
        {
            System.out.println(  aliasAdder.process( appidWithAliasExpr ) );
        }
        catch ( Exception e )
        {
            System.out.println(  "aliasAdder.process( appidWithAliasExpr )" );
            e.printStackTrace();
        }



        //
        // Expressions that should fail
        //
        try
        {
            final String sumImpWithAlias = "sum(r.imp) as sum_imp";
            sqlParser.createExpression( sumImpWithAlias );
        }
        catch (Exception e)
        {
            System.out.println(  "sum(r.imp) as sum_imp" );
            e.printStackTrace();
        }

        try
        {
            final String twoCols = "app_id, campaign_id";
            sqlParser.createExpression( twoCols );
        }
        catch ( Exception e )
        {
            System.out.println(  "app_id, campaign_id" );
            e.printStackTrace();
        }
    }


}
