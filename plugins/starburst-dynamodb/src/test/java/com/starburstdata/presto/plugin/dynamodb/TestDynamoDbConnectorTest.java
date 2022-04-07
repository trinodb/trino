/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableSet;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.assertions.Assert;

import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamoDbConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDynamoDbServer server = closeAfterClass(new TestingDynamoDbServer());
        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_ARRAY:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_DELETE:
            case SUPPORTS_ROW_LEVEL_DELETE:
            case SUPPORTS_CANCELLATION:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_INSERT:
            case SUPPORTS_TRUNCATE:
            case SUPPORTS_ADD_COLUMN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "orderkey")
                .row("custkey", "bigint", "", "custkey")
                .row("orderstatus", "varchar(1)", "", "orderstatus")
                .row("totalprice", "double", "", "totalprice")
                .row("orderdate", "date", "", "orderdate")
                .row("orderpriority", "varchar(15)", "", "orderpriority")
                .row("clerk", "varchar(15)", "", "clerk")
                .row("shippriority", "integer", "", "shippriority")
                .row("comment", "varchar(79)", "", "comment")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        Assert.assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint NOT NULL COMMENT 'orderkey',\n" +
                        "   custkey bigint COMMENT 'custkey',\n" +
                        "   orderstatus varchar(1) COMMENT 'orderstatus',\n" +
                        "   totalprice double COMMENT 'totalprice',\n" +
                        "   orderdate date COMMENT 'orderdate',\n" +
                        "   orderpriority varchar(15) COMMENT 'orderpriority',\n" +
                        "   clerk varchar(15) COMMENT 'clerk',\n" +
                        "   shippriority integer COMMENT 'shippriority',\n" +
                        "   comment varchar(79) COMMENT 'comment'\n" +
                        ")");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (ImmutableSet.of("atrailingspace ", " aleadingspace", "a.dot", "a,comma", "a\"quote", "a\\backslash`").contains(columnName)) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.startsWith("decimal") || typeName.equals("char(3)") || typeName.startsWith("time")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "orderkey")
                .row("custkey", "bigint", "", "custkey")
                .row("orderstatus", "varchar(1)", "", "orderstatus")
                .row("totalprice", "double", "", "totalprice")
                .row("orderdate", "date", "", "orderdate")
                .row("orderpriority", "varchar(15)", "", "orderpriority")
                .row("clerk", "varchar(15)", "", "clerk")
                .row("shippriority", "integer", "", "shippriority")
                .row("comment", "varchar(79)", "", "comment")
                .build();

        assertEquals(expectedParametrizedVarchar, actual, format("%s does not match %s", actual, expectedParametrizedVarchar));
    }
}
