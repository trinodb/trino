package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.tpch.TpchTable;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.util.List;
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
                .addRoundTrip("int64","1", BigintType.BIGINT,"CAST(1 AS BIGINT)")
                .execute(getQueryRunner(), spannerCreateAndInsert("test_boolean"));
    }

    private DataSetup spannerCreateAndInsert(String tableNamePrefix)
    {
        return inputs -> new SpannerTestTable(new JdbcSqlExecutor(spannerInstance.getJdbcUrl(), new Properties())
                ,tableNamePrefix, String.format("%s", getColumns(inputs)));
    }
    @NotNull
    private String getColumns(List<ColumnSetup> inputs)
    {
        return IntStream.range(0, inputs.size())
                .mapToObj(f -> String.format("col_%s %s", f,
                        inputs.get(f).getDeclaredType().get()))
                .collect(Collectors.joining(", "));
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }
}
