import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.Test;

public class TestSpannerDataTypes
{
    @Test
    public void testDataTypes()
            throws Exception
    {
        DistributedQueryRunner queryRunner = SpannerSqlQueryRunner.getQueryRunner();
        queryRunner.execute("DROP TABLE IF EXISTS spanner.default.dTest");
        queryRunner.execute("create table spanner.default.dTest" +
                "( id int,name varchar,is_active boolean)" +
                "WITH (primary_keys = ARRAY['id']," +
                "not_null_fields=ARRAY['name','is_active'])");
        queryRunner.execute("insert into spanner.default.dtest values(1,'Tom',true)");
        MaterializedResult execute = queryRunner.execute("select \"id\" from spanner.default.dtest");
        System.out.println(execute);
    }
    @Test
    public void testTimestamp()
            throws Exception
    {
        DistributedQueryRunner queryRunner = SpannerSqlQueryRunner.getQueryRunner();
        queryRunner.execute("DROP TABLE IF EXISTS spanner.default.dTest");
        queryRunner.execute("create table spanner.default.dTest" +
                "( id int,name varchar,is_active boolean,ts timestamp," +
                "LastContactDate DATE,PopularityScore DOUBLE)" +
                "WITH (primary_keys = ARRAY['id']," +
                "not_null_fields=ARRAY['name','is_active','ts'])");

        queryRunner.execute("insert into spanner.default.dtest values(1,'Tom',true," +
                "CURRENT_TIMESTAMP,CURRENT_DATE,1.111)");
        queryRunner.execute("insert into spanner.default.dtest values(2,'Tom cat',true," +
                "NULL,CURRENT_DATE,1.111)");

        MaterializedResult execute = queryRunner.execute("select * from spanner.default.dtest");
        System.out.println(execute);

    }
}
