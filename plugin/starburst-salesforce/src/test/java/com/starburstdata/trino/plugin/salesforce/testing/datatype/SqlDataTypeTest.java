/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce.testing.datatype;

import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.ResultAssert;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

// Copied from trino-testing but modified to 1) handle the __c suffixes and 2) reset the metadata cache
public final class SqlDataTypeTest
{
    public static SqlDataTypeTest create()
    {
        return new SqlDataTypeTest();
    }

    private final List<TestCase> testCases = new ArrayList<>();

    private SqlDataTypeTest() {}

    public SqlDataTypeTest addRoundTrip(String inputType, String inputLiteral, Type expectedType, String expectedLiteral)
    {
        testCases.add(new TestCase(Optional.of(inputType), inputLiteral, Optional.of(expectedType), expectedLiteral));
        return this;
    }

    public SqlDataTypeTest execute(QueryRunner queryRunner, DataSetup dataSetup)
    {
        return execute(queryRunner, queryRunner.getDefaultSession(), dataSetup);
    }

    public SqlDataTypeTest execute(QueryRunner queryRunner, Session session, DataSetup dataSetup)
    {
        checkState(!testCases.isEmpty(), "No test cases");
        try (TemporaryRelation testTable = dataSetup.setupTemporaryRelation(unmodifiableList(testCases))) {
            verifySelect(queryRunner, session, testTable);
            verifyPredicate(queryRunner, session, testTable);
        }
        return this;
    }

    private void verifySelect(QueryRunner queryRunner, Session session, TemporaryRelation testTable)
    {
        @SuppressWarnings("resource") // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);

        // Build a list of columns as Salesforce does not respect the column order in the DDL when running select *
        String columns = IntStream.range(0, testCases.size())
                .mapToObj(column -> format("col_%d__c", column))
                .collect(joining(", "));

        ResultAssert assertion = assertThat(queryAssertions.query(session, "SELECT " + columns + " FROM " + testTable.getName() + "__c"))
                .result();
        MaterializedResult expected = queryRunner.execute(session, testCases.stream()
                .map(TestCase::getExpectedLiteral)
                .collect(joining(",", "VALUES (", ")")));

        // Verify types if specified
        for (int column = 0; column < testCases.size(); column++) {
            TestCase testCase = testCases.get(column);
            if (testCase.getExpectedType().isPresent()) {
                Type expectedType = testCase.getExpectedType().get();
                assertion.hasType(column, expectedType);
                assertThat(expected.getTypes())
                        .as("Expected literal type (check consistency of expected type and expected literal)")
                        .element(column).isEqualTo(expectedType);
            }
        }

        assertion.matches(expected);
    }

    private void verifyPredicate(QueryRunner queryRunner, Session session, TemporaryRelation testTable)
    {
        String queryWithAll = "SELECT 'all found' FROM " + testTable.getName() + "__c WHERE " +
                IntStream.range(0, testCases.size())
                        .mapToObj(this::getPredicate)
                        .collect(joining(" AND "));

        MaterializedResult result = queryRunner.execute(session, queryWithAll);
        if (result.getOnlyColumnAsSet().equals(Set.of("all found"))) {
            return;
        }

        @SuppressWarnings("resource") // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);

        for (int column = 0; column < testCases.size(); column++) {
            assertThat(queryAssertions.query(session, "SELECT 'found' FROM " + testTable.getName() + "__c WHERE " + getPredicate(column)))
                    .matches("VALUES 'found'");
        }
    }

    private String getPredicate(int column)
    {
        return format("col_%s__c IS NOT DISTINCT FROM %s", column, testCases.get(column).getExpectedLiteral());
    }

    private static class TestCase
            implements ColumnSetup
    {
        private final Optional<String> declaredType;
        private final String inputLiteral;
        private final Optional<Type> expectedType;
        private final String expectedLiteral;

        public TestCase(Optional<String> declaredType, String inputLiteral, Optional<Type> expectedType, String expectedLiteral)
        {
            this.declaredType = requireNonNull(declaredType, "declaredType is null");
            this.expectedType = requireNonNull(expectedType, "expectedType is null");
            this.inputLiteral = requireNonNull(inputLiteral, "inputLiteral is null");
            this.expectedLiteral = requireNonNull(expectedLiteral, "expectedLiteral is null");
        }

        @Override
        public Optional<String> getDeclaredType()
        {
            return declaredType;
        }

        @Override
        public String getInputLiteral()
        {
            return inputLiteral;
        }

        public Optional<Type> getExpectedType()
        {
            return expectedType;
        }

        public String getExpectedLiteral()
        {
            return expectedLiteral;
        }
    }
}
