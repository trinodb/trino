/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb.testing;

import com.starburstdata.presto.plugin.dynamodb.DynamoDbConfig;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TemporaryRelation;
import io.trino.testing.sql.TestTable;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.google.common.base.Throwables.getCausalChain;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DynamoDbDataSetup
        implements DataSetup
{
    private static final Logger log = Logger.get(DynamoDbDataSetup.class);

    private final DynamoDbConfig config;
    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;

    private static final RetryPolicy<SqlExecutor> QUERY_EXECUTION_RETRY_POLICY = RetryPolicy.<SqlExecutor>builder()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxRetries(5)
            .handleIf(throwable -> getCausalChain(throwable).stream()
                    .anyMatch(SQLException.class::isInstance))
            .onRetry(event -> log.warn(
                    "Query failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastException().getMessage()))
            .build();

    public DynamoDbDataSetup(DynamoDbConfig config, SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        this.config = requireNonNull(config, "config is null");
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.tableNamePrefix = requireNonNull(tableNamePrefix, "tableNamePrefix is null");
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TestTable testTable = createTestTable(inputs);
        try {
            insertRows(testTable, inputs);
        }
        catch (Exception e) {
            closeAllSuppress(e, testTable);
            throw e;
        }
        return testTable;
    }

    private void insertRows(TestTable testTable, List<ColumnSetup> inputs)
    {
        StringBuilder columnLiterals = new StringBuilder();
        StringBuilder valueLiterals = new StringBuilder();

        // JDBC Driver does not let you insert a literal null using a standard statement, so skip any null literals
        int i = 0;
        for (ColumnSetup setup : inputs) {
            if (!setup.getInputLiteral().equals("null")) {
                if (columnLiterals.length() > 0) {
                    columnLiterals.append(", ");
                    valueLiterals.append(", ");
                }
                columnLiterals.append("col_").append(i++);
                valueLiterals.append(setup.getInputLiteral());
            }
        }

        String sql = format("INSERT INTO %s (%s) VALUES (%s)", testTable.getName(), columnLiterals, valueLiterals);

        // CData driver will intermittently throw a SQLException with no error message
        // Retrying the query typically works
        Failsafe.with(QUERY_EXECUTION_RETRY_POLICY)
                .run(() -> sqlExecutor.execute(sql));
    }

    private DynamoDbTestTable createTestTable(List<ColumnSetup> inputs)
    {
        return new DynamoDbTestTable(config, sqlExecutor, tableNamePrefix, inputs);
    }
}
