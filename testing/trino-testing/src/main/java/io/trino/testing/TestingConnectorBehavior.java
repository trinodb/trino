/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public enum TestingConnectorBehavior
{
    SUPPORTS_INSERT,
    SUPPORTS_DELETE,
    SUPPORTS_ROW_LEVEL_DELETE(SUPPORTS_DELETE),
    SUPPORTS_UPDATE,
    SUPPORTS_MERGE,

    SUPPORTS_TRUNCATE(SUPPORTS_DELETE),

    SUPPORTS_ARRAY,
    SUPPORTS_ROW_TYPE,

    SUPPORTS_PREDICATE_PUSHDOWN,
    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY(SUPPORTS_PREDICATE_PUSHDOWN),
    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY(SUPPORTS_PREDICATE_PUSHDOWN),
    SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN(SUPPORTS_PREDICATE_PUSHDOWN),
    SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN(SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN),
    SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE(SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN),

    SUPPORTS_DYNAMIC_FILTER_PUSHDOWN(false),

    SUPPORTS_LIMIT_PUSHDOWN,

    SUPPORTS_TOPN_PUSHDOWN,
    SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR(and(SUPPORTS_TOPN_PUSHDOWN, SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY)),

    SUPPORTS_AGGREGATION_PUSHDOWN,
    SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV(SUPPORTS_AGGREGATION_PUSHDOWN),
    SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE(SUPPORTS_AGGREGATION_PUSHDOWN),
    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE(SUPPORTS_AGGREGATION_PUSHDOWN),
    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION(SUPPORTS_AGGREGATION_PUSHDOWN),
    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION(SUPPORTS_AGGREGATION_PUSHDOWN),
    SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT(SUPPORTS_AGGREGATION_PUSHDOWN),

    SUPPORTS_JOIN_PUSHDOWN(
            // Currently no connector supports Join pushdown by default. JDBC connectors may support Join pushdown and BaseJdbcConnectorTest
            // verifies truthfulness of SUPPORTS_JOIN_PUSHDOWN declaration, so it is a safe default.
            false),
    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN(SUPPORTS_JOIN_PUSHDOWN),
    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM(SUPPORTS_JOIN_PUSHDOWN),
    SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY(and(SUPPORTS_JOIN_PUSHDOWN, SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY)),
    SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY(and(SUPPORTS_JOIN_PUSHDOWN, SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY)),

    SUPPORTS_DEREFERENCE_PUSHDOWN(SUPPORTS_ROW_TYPE),

    SUPPORTS_CREATE_SCHEMA,
    // Expect rename to be supported when create schema is supported, to help make connector implementations coherent.
    SUPPORTS_RENAME_SCHEMA(SUPPORTS_CREATE_SCHEMA),
    SUPPORTS_DROP_SCHEMA_CASCADE(SUPPORTS_CREATE_SCHEMA),

    SUPPORTS_CREATE_TABLE,
    SUPPORTS_CREATE_TABLE_WITH_DATA(SUPPORTS_CREATE_TABLE),
    SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT(SUPPORTS_CREATE_TABLE),
    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT(SUPPORTS_CREATE_TABLE),
    SUPPORTS_RENAME_TABLE,
    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS(SUPPORTS_RENAME_TABLE),

    SUPPORTS_ADD_COLUMN,
    SUPPORTS_ADD_COLUMN_WITH_COMMENT(SUPPORTS_ADD_COLUMN),
    SUPPORTS_ADD_FIELD(fallback -> fallback.test(SUPPORTS_ADD_COLUMN) && fallback.test(SUPPORTS_ROW_TYPE)),
    SUPPORTS_DROP_COLUMN(SUPPORTS_ADD_COLUMN),
    SUPPORTS_DROP_FIELD(and(SUPPORTS_DROP_COLUMN, SUPPORTS_ROW_TYPE)),
    SUPPORTS_RENAME_COLUMN,
    SUPPORTS_RENAME_FIELD(fallback -> fallback.test(SUPPORTS_RENAME_COLUMN) && fallback.test(SUPPORTS_ROW_TYPE)),
    SUPPORTS_SET_COLUMN_TYPE,
    SUPPORTS_SET_FIELD_TYPE(fallback -> fallback.test(SUPPORTS_SET_COLUMN_TYPE) && fallback.test(SUPPORTS_ROW_TYPE)),

    SUPPORTS_COMMENT_ON_TABLE,
    SUPPORTS_COMMENT_ON_COLUMN(SUPPORTS_COMMENT_ON_TABLE),

    SUPPORTS_CREATE_VIEW,
    SUPPORTS_COMMENT_ON_VIEW(and(SUPPORTS_CREATE_VIEW, SUPPORTS_COMMENT_ON_TABLE)),
    SUPPORTS_COMMENT_ON_VIEW_COLUMN(SUPPORTS_COMMENT_ON_VIEW),

    SUPPORTS_CREATE_MATERIALIZED_VIEW,
    SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD(SUPPORTS_CREATE_MATERIALIZED_VIEW),
    SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW(SUPPORTS_CREATE_MATERIALIZED_VIEW), // i.e. an MV that spans catalogs
    SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES(SUPPORTS_CREATE_MATERIALIZED_VIEW),
    SUPPORTS_RENAME_MATERIALIZED_VIEW(SUPPORTS_CREATE_MATERIALIZED_VIEW),
    SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS(SUPPORTS_RENAME_MATERIALIZED_VIEW),
    SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN(SUPPORTS_CREATE_MATERIALIZED_VIEW),

    SUPPORTS_NOT_NULL_CONSTRAINT(SUPPORTS_CREATE_TABLE),
    SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT(and(SUPPORTS_NOT_NULL_CONSTRAINT, SUPPORTS_ADD_COLUMN)),

    SUPPORTS_NEGATIVE_DATE,

    SUPPORTS_CANCELLATION(false),

    SUPPORTS_MULTI_STATEMENT_WRITES(false),

    SUPPORTS_NATIVE_QUERY(true), // system.query or equivalent PTF for query passthrough

    SUPPORTS_REPORTING_WRITTEN_BYTES(false),

    /**/;

    private final Predicate<Predicate<TestingConnectorBehavior>> hasBehaviorByDefault;

    TestingConnectorBehavior()
    {
        this(true);
    }

    TestingConnectorBehavior(boolean hasBehaviorByDefault)
    {
        this(fallback -> hasBehaviorByDefault);
        checkArgument(
                !hasBehaviorByDefault ==
                        // TODO make these marked as expected by default
                        (name().equals("SUPPORTS_CANCELLATION") ||
                                name().equals("SUPPORTS_DYNAMIC_FILTER_PUSHDOWN") ||
                                name().equals("SUPPORTS_JOIN_PUSHDOWN") ||
                                name().equals("SUPPORTS_REPORTING_WRITTEN_BYTES") ||
                                name().equals("SUPPORTS_MULTI_STATEMENT_WRITES")),
                "Every behavior should be expected to be true by default. Having mixed defaults makes reasoning about tests harder. False default provided for %s",
                name());
    }

    TestingConnectorBehavior(TestingConnectorBehavior defaultBehaviorSource)
    {
        this(fallback -> fallback.test(defaultBehaviorSource));
    }

    TestingConnectorBehavior(Predicate<Predicate<TestingConnectorBehavior>> hasBehaviorByDefault)
    {
        this.hasBehaviorByDefault = requireNonNull(hasBehaviorByDefault, "hasBehaviorByDefault is null");
    }

    /**
     * Determines whether a connector should be assumed to have this behavior.
     *
     * @param fallback callback to be used to inspect connector behaviors, if this behavior is assumed to be had conditionally
     */
    // Intentionally package private. This is just a convenience method for sharing behavior between reusable test base classes.
    // If it was to be public, a different API would need to be provided.
    boolean hasBehaviorByDefault(Predicate<TestingConnectorBehavior> fallback)
    {
        return hasBehaviorByDefault.test(fallback);
    }

    private static Predicate<Predicate<TestingConnectorBehavior>> and(TestingConnectorBehavior first, TestingConnectorBehavior... rest)
    {
        List<TestingConnectorBehavior> conjuncts = ImmutableList.copyOf(Lists.asList(first, rest));
        return fallback -> conjuncts.stream().allMatch(fallback);
    }
}
