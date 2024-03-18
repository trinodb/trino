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

package io.trino.tests.product.warp.utils;

import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.tests.product.warp.utils.syntheticconfig.TableType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record TestFormat(String name, int lines, String table_name, List<Column> structure, List<WarmupRule> warmup_rules,
                         Map<String, Object> session_properties, String warm_query, boolean skip, boolean pt_enable, boolean skip_caching,
                         List<QueryData> queries_data, WarmTypeForStrings warm_type_for_strings, String description,
                         int expected_warm_failures, Map<String, Long> expected_dictionary_counters, List<TableType> skip_type,
                         Map<String, Long> iceberg_expected_dictionary_counters, Map<String, Long> dl_expected_dictionary_counters, int split_count, List<Object> bucketed_by, int bucket_count)
{
    public record Column(String name, String type, List<Object> args) {}

    public record WarmupRule(String colNameId, List<String> predicates, List<WarmUpType> warmUpTypes) {}

    public record QueryData(String query, List<Object> expected_result, String query_id,
                            Map<String, Long> expected_counters, Map<String, Object> session_properties,
                            boolean skip, List<Object> expected_iceberg_result, List<Object> expected_dl_result,
                            Map<String, Long> iceberg_expected_counters, Map<String, Long> dl_expected_counters) {}

    public String getTableName()
    {
        return table_name() != null ? table_name() : name();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(TestFormat testFormat)
    {
        return new Builder()
                .name(testFormat.name())
                .lines(testFormat.lines())
                .tableName(testFormat.table_name())
                .structure(testFormat.structure())
                .warmupRules(testFormat.warmup_rules())
                .sessionProperties(testFormat.session_properties())
                .warmQuery(testFormat.warm_query())
                .skip(testFormat.skip())
                .ptEnable(testFormat.pt_enable())
                .skipCaching(testFormat.skip_caching())
                .queriesData(testFormat.queries_data())
                .warmTypeForStrings(testFormat.warm_type_for_strings())
                .description(testFormat.description())
                .splitCount(testFormat.split_count())
                .bucketCount(testFormat.bucket_count())
                .bucketedBy(testFormat.bucketed_by())
                .expectedWarmFailures(testFormat.expected_warm_failures())
                .expectedDictionaryCounters(testFormat.expected_dictionary_counters())
                .skipType(testFormat.skip_type())
                .expectedIcebergDictionaryCounters(testFormat.iceberg_expected_dictionary_counters())
                .expectedDLDictionaryCounters(testFormat.dl_expected_dictionary_counters());
    }

    //The test format that we got from the json, should be changed in case table type other than warp (DL or Icebrg)
    //was provided in the test command line
    //The table name, expected results and expected counters sometimes are affect by table type
    public TestFormat withTableType(String updatedName, TableType tableType)
    {
        List<QueryData> updatedQueriesData = new ArrayList<>();
        Map<String, Long> updatedDictionaryCounters = expected_dictionary_counters;

        for (QueryData queryData : queries_data()) {
            updatedQueriesData.add(getUpdatedQueryData(tableType, queryData, updatedName));
        }
        String updatedWarmQuery = null;
        if (warm_query != null) {
            updatedWarmQuery = warm_query.replace(getTableName(), updatedName);
        }
        String updatedTableName = null;
        if (table_name != null) {
            updatedTableName = updatedName;
        }

        if (tableType == TableType.varada_delta_lake && dl_expected_dictionary_counters != null) {
            updatedDictionaryCounters = dl_expected_dictionary_counters;
        }
        else if (tableType == TableType.varada_iceberg && iceberg_expected_dictionary_counters != null) {
            updatedDictionaryCounters = iceberg_expected_dictionary_counters;
        }

        return builder(this)
                .name(updatedName)
                .tableName(updatedTableName)
                .warmQuery(updatedWarmQuery)
                .queriesData(updatedQueriesData)
                .expectedDictionaryCounters(updatedDictionaryCounters)
                .build();
    }

    private QueryData getUpdatedQueryData(TableType tableType, QueryData queryData, String updatedName)
    {
        List<Object> expectedResult = new ArrayList<>(queryData.expected_result());
        Map<String, Long> expectedCounters = queryData.expected_counters();
        String updatedQuery = getTableName();

        if (updatedName != null && !updatedName.isEmpty()) {
            updatedQuery = queryData.query().replace(getTableName(), updatedName);
        }
        if (tableType == TableType.varada_delta_lake) {
            if (queryData.expected_dl_result() != null) {
                expectedResult = new ArrayList<>(queryData.expected_dl_result());
            }
            if (queryData.dl_expected_counters() != null) {
                expectedCounters = queryData.dl_expected_counters();
            }
        }
        else if (tableType == TableType.varada_iceberg) {
            if (queryData.expected_iceberg_result() != null) {
                expectedResult = new ArrayList<>(queryData.expected_iceberg_result());
            }
            if (queryData.iceberg_expected_counters() != null) {
                expectedCounters = queryData.iceberg_expected_counters();
            }
        }

        return new QueryData(updatedQuery, expectedResult,
                queryData.query_id, expectedCounters, queryData.session_properties, queryData.skip,
                null, null, null, null);
    }

    public static class Builder
    {
        private String name;
        private int lines;
        private String tableName;
        private List<Column> structure;
        private List<WarmupRule> warmupRules;
        private Map<String, Object> sessionProperties;
        private String warmQuery;
        private boolean skip;
        private boolean ptEnable = true;
        private boolean skipCaching;
        private List<QueryData> queriesData;
        private WarmTypeForStrings warmTypeForStrings;
        private String description;
        private int expectedWarmFailures;
        private Map<String, Long> expectedDictionaryCounters;
        private List<TableType> skipType;
        private Map<String, Long> expectedIcebergDictionaryCounters;
        private Map<String, Long> expectedDLDictionaryCounters;
        private int splitCount;

        private List<Object> bucketedBy;
        private int bucketCount;

        private Builder()
        {
        }

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder lines(int lines)
        {
            this.lines = lines;
            return this;
        }

        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder structure(List<Column> structure)
        {
            this.structure = structure;
            return this;
        }

        public Builder warmupRules(List<WarmupRule> warmupRules)
        {
            this.warmupRules = warmupRules;
            return this;
        }

        public Builder sessionProperties(Map<String, Object> sessionProperties)
        {
            this.sessionProperties = sessionProperties;
            return this;
        }

        public Builder warmQuery(String warmQuery)
        {
            this.warmQuery = warmQuery;
            return this;
        }

        public Builder skip(boolean skip)
        {
            this.skip = skip;
            return this;
        }

        public Builder ptEnable(boolean ptEnable)
        {
            this.ptEnable = ptEnable;
            return this;
        }

        public Builder skipCaching(boolean skipCaching)
        {
            this.skipCaching = skipCaching;
            return this;
        }

        public Builder queriesData(List<QueryData> queriesData)
        {
            this.queriesData = queriesData;
            return this;
        }

        public Builder warmTypeForStrings(WarmTypeForStrings warmTypeForStrings)
        {
            this.warmTypeForStrings = warmTypeForStrings;
            return this;
        }

        public Builder description(String description)
        {
            this.description = description;
            return this;
        }

        public Builder expectedWarmFailures(int expectedWarmFailures)
        {
            this.expectedWarmFailures = expectedWarmFailures;
            return this;
        }

        public Builder expectedDictionaryCounters(Map<String, Long> expectedDictionaryCounters)
        {
            this.expectedDictionaryCounters = expectedDictionaryCounters;
            return this;
        }

        public Builder skipType(List<TableType> skipType)
        {
            this.skipType = skipType;
            return this;
        }

        public Builder expectedIcebergDictionaryCounters(Map<String, Long> expectedIcebergDictionaryCounters)
        {
            this.expectedIcebergDictionaryCounters = expectedIcebergDictionaryCounters;
            return this;
        }

        public Builder expectedDLDictionaryCounters(Map<String, Long> expectedDLDictionaryCounters)
        {
            this.expectedDLDictionaryCounters = expectedDLDictionaryCounters;
            return this;
        }

        public Builder splitCount(int splitCount)
        {
            this.splitCount = splitCount;
            return this;
        }

        public Builder bucketedBy(List<Object> bucketedBy)
        {
            this.bucketedBy = bucketedBy;
            return this;
        }

        public Builder bucketCount(int bucketCount)
        {
            this.bucketCount = bucketCount;
            return this;
        }

        public TestFormat build()
        {
            return new TestFormat(name, lines, tableName, structure, warmupRules, sessionProperties, warmQuery,
                    skip, ptEnable, skipCaching, queriesData, warmTypeForStrings, description,
                    expectedWarmFailures, expectedDictionaryCounters, skipType, expectedIcebergDictionaryCounters,
                    expectedDLDictionaryCounters, splitCount, bucketedBy, bucketCount);
        }
    }
}
