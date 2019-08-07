package io.prestosql.sql.hive;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.ShowSchemas;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.parser.IdentifierSymbol.COLON;

public class TestHiveSqlParser {

    private SqlParser sqlParser = null;
    private ParsingOptions hiveParsingOptions = null;
    private ParsingOptions prestoParsingOptions = null;
    @BeforeTest
    public void init() {
        sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));
        hiveParsingOptions = new ParsingOptions();
        hiveParsingOptions.setIfUseHiveParser(true);
        prestoParsingOptions = new ParsingOptions();
        prestoParsingOptions.setIfUseHiveParser(false);
    }

    @Test
    public void testUse()
    {
        String sql = "USE hive.tmp";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testSetSession()
    {
        String sql = "SET SESSION foo=true";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testHiveSimple()
    {
        String sql = "SELECT x FROM `t.x.m`";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testPrestoSimple()
    {
        String sql = "SELECT x FROM t.x.m";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testSelect01()
    {
        String sql = "SELECT \"a\",b,c,d FROM ALGO.t WHERE x=321 LIMIT 100";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testQuotedQuery()
    {
        String sql = "SELECT `a`,b,c,d FROM ALGO.t WHERE x=321 LIMIT 100";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }
    @Test
    public void testDoubleEq()
    {
        String sql = "SELECT `a`,b,c,d FROM ALGO.t WHERE x==321 LIMIT 100";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }
    @Test
    public void testTableStartWithDigit()
    {
        String sql = "select * from TMP.20171014_tmpdata limit 10";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testSplit()
    {
        String sql = "\"split\"(\"registertime\", ' ')[BIGINT '1']";
        Node node = sqlParser.createExpression(sql);
        System.out.println(node);
    }

    @Test
    public void testBinnary()
    {
        String sql = "select 111|112 as x from bigolive.presto_job_audit where day='2019-07-26' limit 1";
        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testUnnestWithOrdinality()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (pos, event)";

        Node node = sqlParser.createStatement(prestoSql, prestoParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLateralViewWithOrdinality()
    {
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view posexplode(events) t1 as pos, event";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testUnnestMultiColumn()
    {
        String prestoSql = "" +
                "SELECT event.*, event1.*" +
                "FROM tb1 " +
                "CROSS JOIN UNNEST(events1) WITH ORDINALITY AS event1 (c1) " +
                "CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (c)";

        Node node = sqlParser.createStatement(prestoSql, prestoParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLateralViewMultiColumn()
    {
        String hiveSql = "" +
                "SELECT event.*, event1.*" +
                "FROM tb1 " +
                "lateral view explode(events) event as c " +
                "lateral view explode(events1) event1 as c1";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testLateralViewMultiColumn1()
    {
        String hiveSql = "SELECT numbers, animals,c,c2\n" +
                "FROM (\n" +
                "  VALUES\n" +
                "    (ARRAY[2, 5], ARRAY['dog', 'cat', 'bird']),\n" +
                "    (ARRAY[7, 8, 9], ARRAY['cow', 'pig'])\n" +
                ") AS x (numbers, animals)\n" +
                "lateral view explode(numbers) t as c\n" +
                " lateral view explode(animals) t1 as c1";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testComplexQuery1()
    {
        String hiveSql = "select\n" +
                "session_id,\n" +
                "uid,\n" +
                "hdid,\n" +
                "country,\n" +
                "client_version,\n" +
                "phone_lang,\n" +
                "os,\n" +
                "net_type,\n" +
                "login_status,\n" +
                "market_source,\n" +
                "follow_source,\n" +
                "followed_uid,\n" +
                "sum(follow_count_01) as follow_count_01,\n" +
                "sum(cancel_follow_count_01) as cancel_follow_count_01\n" +
                "\n" +
                "from\n" +
                "\n" +
                "(\n" +
                "  select\n" +
                "  if(session_id is null or session_id = '','unknown',trim(session_id)) as session_id,\n" +
                "  uid,\n" +
                "  if(hdid is null or hdid = '','unknown',trim(hdid)) as hdid,\n" +
                "  if(country is null or country = '','unknown',trim(country)) as country,\n" +
                "  if(client_version is null or client_version = '','unknown',trim(client_version)) as client_version,\n" +
                "  if(locale is null or locale = '','unknown',trim(locale)) as phone_lang,\n" +
                "  if(os is null or os = '','unknown',trim(os)) as os,\n" +
                "  if(event.net is null or event.net = '','unknown',trim(event.net)) as net_type,\n" +
                "  login_state as login_status,\n" +
                "  if(market_source is null or market_source = '','unknown',trim(market_source)) as market_source,\n" +
                "  if(event.event_info['source'] < 0 or event.event_info['source'] is null or event.event_info['source'] ='' ,'unknown',trim(event.event_info['source'])) as follow_source,\n" +
                "  cast(event.event_info['follow_uid'] as bigint) as followed_uid,\n" +
                "  if(event.event_info['action']='1', 1, 0) as follow_count_01,\n" +
                "  if(event.event_info['action']='2', 1, 0) as cancel_follow_count_01\n" +
                "  from vlog.like_user_event_orc\n" +
                "  where event_id='0104013' and day='2019-07-21' and !(uid is null and (hdid is null or hdid=''))\n" +
                ") t0\n" +
                "group by session_id,uid,hdid,country,client_version,phone_lang,os,net_type,login_status,market_source,follow_source,followed_uid LIMIT 10";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }


    @Test
    public void testPrestoWithCube()
    {
        String hiveSql = "SELECT day, user, count(1)\n" +
                "FROM bigolive.presto_job_audit\n" +
                "where user = 'weijing'\n" +
                "GROUP by cube(day, user)\n" +
                "limit 10";

        Node node = sqlParser.createStatement(hiveSql, prestoParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testHiveWithCube()
    {
        String hiveSql = "SELECT day, `user`, count(1)\n" +
                "FROM bigolive.presto_job_audit\n" +
                "where user == \"weijing\"\n" +
                "GROUP by `day`, user with cube\n" +
                "limit 10";

        Node node = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(node);
    }

    @Test
    public void testHiveShowDatabases()
    {
        String hiveSql = "SHOW DATABASES like '%tmp%'";
        String prestoSql = "SHOW SCHEMAS like '%tmp%'";
        Node hiveNode = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        Node prestoNode = sqlParser.createStatement(hiveSql, hiveParsingOptions);
        System.out.println(hiveSql);
    }

}