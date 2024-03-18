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

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.query.QueryResult;
import org.intellij.lang.annotations.Language;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class JMXCachingManager
{
    static {
        try {
            init();
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Class<?>, List<String>> stats;
    private static Map<String, String> counterToErrorMessage;

    private JMXCachingManager()
    {
    }

    public static List<String> getStatsNames(Class<?> clazz)
    {
        return stats.get(clazz);
    }

    private static List<String> getFields(Class<?> clazz)
            throws IllegalAccessException
    {
        List<String> fieldList = new ArrayList<>();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            fieldList.add(String.valueOf(field.get(null)));
        }

        return fieldList;
    }

    public static synchronized void init()
            throws IllegalAccessException
    {
        if (stats != null) {
            return;
        }
        stats = new HashMap<>();
        Class<?> clazz = JMXCachingConstants.class;
        Class<?>[] innerClasses = clazz.getDeclaredClasses();

        List<Class<?>> statTables = new ArrayList<>();
        Collections.addAll(statTables, innerClasses);

        for (var statTable : statTables) {
            List<String> fields = getFields(statTable);
            stats.put(statTable, fields);
        }

        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        mapBuilder.put(JMXCachingConstants.Columns.VARADA_MATCH, "incorrect match - expected to match varada columns")
                .put(JMXCachingConstants.Columns.VARADA_MATCH_ON_SIMPLIFIED_DOMAIN, "incorrect match - expected to match on simplified domain")
                .put(JMXCachingConstants.Columns.VARADA_COLLECT, "incorrect collect - expected to collect varada columns")
                .put(JMXCachingConstants.Columns.VARADA_MATCH_COLLECT, "incorrect collect - expected to match-collect varada columns")
                .put(JMXCachingConstants.Columns.PREFILLED_COLLECT, "incorrect collect - expected to prefilled collect columns")
                .put(JMXCachingConstants.Columns.EXTERNAL_MATCH, "incorrect match - expected to match hive columns")
                .put(JMXCachingConstants.Columns.EXTERNAL_COLLECT, "incorrect collect - expected to collect hive columns")
                .put(JMXCachingConstants.DispatcherPageSource.HIVE, "incorrect cached hive files - expected to go hive only")
                .put(JMXCachingConstants.DispatcherPageSource.VARADA_SUCCESS, "incorrect cached varada success files - expected to go varada only")
                .put(JMXCachingConstants.DispatcherPageSource.EMPTY_COLLECT_COLUMNS, "Incorrect partition collect")
                .put(JMXCachingConstants.DispatcherPageSource.FILTERED_BY_PREDICATE, "Incorrect filter by predicate")
                .put(JMXCachingConstants.DispatcherPageSource.NON_TRIVIAL_ALTERNATIVE_CHOSEN, "Incorrect non trivial alternative chosen");
        counterToErrorMessage = mapBuilder.buildOrThrow();
    }

    public static long getValue(QueryResult queryResult, String columnName)
    {
        return (Long) queryResult.column(queryResult.tryFindColumnIndex(columnName).orElseThrow()).get(0);
    }

    public static long getDiffFromInitial(QueryResult after, QueryResult before, String columnName)
    {
        return getValue(after, columnName) - getValue(before, columnName);
    }

    public static String getErrorMessage(String counter)
    {
        return counterToErrorMessage.get(counter);
    }

    public static QueryResult getWarmingStats()
    {
        List<String> statsNames = JMXCachingManager.getStatsNames(JMXCachingConstants.WarmingService.class);
        return getServiceStats("jmx.current.\"*name=warming-service.*\"", statsNames);
    }

    public static QueryResult getDictionaryStats()
    {
        List<String> dictionaryStatsNames = JMXCachingManager.getStatsNames(JMXCachingConstants.Dictionary.class);
        return getServiceStats("jmx.current.\"*name=dictionary.*\"", dictionaryStatsNames);
    }

    public static QueryResult getDemoterStats()
    {
        List<String> demoterStatsNames = JMXCachingManager.getStatsNames(JMXCachingConstants.WarmupDemoter.class);
        return getServiceStats("jmx.current.\"*name=warmupDemoter.*\"", demoterStatsNames);
    }

    public static QueryResult getImportStats()
    {
        List<String> importStats = JMXCachingManager.getStatsNames(JMXCachingConstants.WarmupImportService.class);
        return getServiceStats("jmx.current.\"*name=import-service.*\"", importStats);
    }

    public static QueryResult getExportStats()
    {
        List<String> exportStats = JMXCachingManager.getStatsNames(JMXCachingConstants.WarmupExportService.class);
        return getServiceStats("jmx.current.\"*exportservice*\"", exportStats);
    }

    public static QueryResult getQueryStats()
    {
        List<String> exportStats = JMXCachingManager.getStatsNames(JMXCachingConstants.Columns.class);
        return getServiceStats("jmx.current.\"*pagesource*\"", exportStats);
    }

    public static List<String> getQueryStatsNames()
    {
        return stats.get(JMXCachingConstants.Columns.class);
    }

    private static QueryResult getServiceStats(String jmxTable, List<String> statColNames)
    {
        String statSumColNames = statColNames
                .stream()
                .map(x -> "sum(" + x + ")" + " as " + x)
                .collect(Collectors.joining(","));
        @Language("SQL") String format = format("SELECT %s FROM %s", statSumColNames, jmxTable);

        return onTrino().executeQuery(format);
    }
}
