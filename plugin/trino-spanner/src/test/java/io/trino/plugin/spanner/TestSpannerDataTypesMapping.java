package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.BigintType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TemporaryRelation;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.BooleanType.BOOLEAN;

public class TestSpannerDataTypesMapping
        extends AbstractTestQueryFramework
{
    protected TestingSpannerInstance spannerInstance;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        spannerInstance = closeAfterClass(new TestingSpannerInstance());
        return SpannerQueryRunner.createSpannerQueryRunner(
                spannerInstance,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables(), false);
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bool", "true", BOOLEAN)
                .addRoundTrip("bool", "false", BOOLEAN)
                .addRoundTrip("bool", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("int64", "1", BigintType.BIGINT, "CAST(1 as BIGINT)")
                .execute(getQueryRunner(), spannerCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    private DataSetup spannerCreateAndInsert(String tableNamePrefix)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(spannerInstance.getJdbcUrl(), new Properties());
        return inputs -> {

            String primaryKey = String.format("col_%s", inputs.size() - 1);

            jdbcSqlExecutor.execute("CREATE TABLE %s (%s) PRIMARY KEY (%s)"
                    .formatted(
                            tableNamePrefix,
                            IntStream.range(0, inputs.size())
                                    .mapToObj(f -> String.format("col_%s %s", f, inputs.get(f).getDeclaredType().get()))
                                    .collect(Collectors.joining(", ")),
                            primaryKey));
            return new TemporaryRelation()
            {
                @Override
                public String getName()
                {
                    return tableNamePrefix;
                }

                @Override
                public void close()
                {

                }
            };
        };
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }
}
