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
package io.trino.tests.product.warp.utils.syntheticconfig;

import com.google.common.base.Splitter;
import io.trino.tests.product.warp.utils.TestFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class TestConfiguration
{
    public static final String TEST_NAME = "name";
    public static final String QUERY_ID = "query_id";
    public static final String STRATEGY = "strategy";
    public static final String TABLE_TYPE = "table_type";
    public static final String DELTA_TYPE_PREFIX = "dl_";
    public static final String ICEBERG_TYPE_PREFIX = "iceberg_";
    private static Map<String, List<String>> configToTestsConfiguration = new HashMap<>();
    private static final boolean onHiveAlternative = false; //Allow run on hive when the specific type test is disable

    private static final Map<Strategy, TestParser> testsParamsParser = Map.of(
            Strategy.INCLUDE, new IncludeStrategy(),
            Strategy.EXCLUDE, new ExcludeStrategy());

    private TestConfiguration() {}

    public static boolean hasRunConfiguration()
    {
        return !configToTestsConfiguration.isEmpty();
    }

    public static boolean hasTestFilter()
    {
        return configToTestsConfiguration.containsKey(TEST_NAME);
    }

    public static TableType getTableType()
    {
        return TableType.valueOf(configToTestsConfiguration.getOrDefault(TABLE_TYPE, List.of(TableType.warp.name())).getFirst().toUpperCase(Locale.ROOT));
    }

    public static boolean typeNeedBeSkipped(TestFormat test, TableType tableType)
    {
        List<TableType> skipTypes = test.skip_type();
        if (skipTypes == null) {
            return false;
        }
        return skipTypes.stream().anyMatch(skipType -> skipType == tableType);
    }

    public static void setConfiguration(CommandLine warpCommandLine)
    {
        Map<String, List<String>> testConfiguration = new HashMap<>();
        for (Option o : warpCommandLine.getOptions()) {
            List<String> values = Splitter.on(',').splitToList(o.getValue()).stream().toList();
            testConfiguration.computeIfAbsent(o.getArgName(), k -> new ArrayList<>()).addAll(values);
        }
        configToTestsConfiguration = testConfiguration;
    }

    public static List<TestFormat> filterTests(List<TestFormat> tests)
    {
        Strategy strategy = Strategy.valueOf(
                configToTestsConfiguration
                        .getOrDefault(STRATEGY, List.of(Strategy.INCLUDE.name()))
                        .getFirst().toUpperCase(Locale.ROOT));
        return testsParamsParser.get(strategy).parse(configToTestsConfiguration, tests);
    }

    private static String getTableTypePrefix(TableType tableType)
            throws Exception
    {
        return switch (tableType) {
            case varada_iceberg -> ICEBERG_TYPE_PREFIX;
            case varada_delta_lake -> DELTA_TYPE_PREFIX;
            case warp -> "";
            case null -> throw new Exception(String.format("Not expected table type %s", getTableType().name()));
        };
    }

    public static List<TestFormat> updateTableType(List<TestFormat> tests)
            throws Exception
    {
        List<TestFormat> testsUpdated = new ArrayList<>();
        TableType tableType = getTableType();
        if (tableType == TableType.warp) {
            return tests;
        }
        String typePrefix = getTableTypePrefix(tableType);
        for (TestFormat test : tests) {
            if (typeNeedBeSkipped(test, tableType)) {
                if (onHiveAlternative) {
                    testsUpdated.add(test); //Add the original test and ignore table type
                }
                continue;
            }
            String updatedName;
            if (test.table_name() == null) {
                updatedName = typePrefix + test.name();
            }
            else {
                updatedName = typePrefix + test.table_name();
            }
            testsUpdated.add(test.withTableType(updatedName, tableType));
        }
        return testsUpdated;
    }
}
