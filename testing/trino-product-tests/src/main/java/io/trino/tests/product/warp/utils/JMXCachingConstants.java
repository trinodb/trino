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

public class JMXCachingConstants
{
    private JMXCachingConstants() {}

    public static class DispatcherPageSource
    {
        public static final String HIVE = "cached_proxied_files";
        public static final String VARADA_SUCCESS = "cached_varada_success_files";
        public static final String EMPTY_COLLECT_COLUMNS = "empty_collect_columns";
        public static final String FILTERED_BY_PREDICATE = "filtered_by_predicate";
        public static final String NON_TRIVIAL_ALTERNATIVE_CHOSEN = "non_trivial_alternative_chosen";
    }

    public static class Columns
    {
        public static final String VARADA_MATCH = "varada_match_columns";
        public static final String VARADA_MATCH_ON_SIMPLIFIED_DOMAIN = "varada_match_on_simplified_domain";
        public static final String VARADA_COLLECT = "varada_collect_columns";
        public static final String VARADA_MATCH_COLLECT = "varada_match_collect_columns";
        public static final String PREFILLED_COLLECT = "prefilled_collect_columns";
        public static final String EXTERNAL_MATCH = "external_match_columns";
        public static final String EXTERNAL_COLLECT = "external_collect_columns";
        public static final String WARP_CACHE_SKIPPED = "skip_warp_cache_manager";
        public static final String WARP_CACHE_SKIPPED_DYNAMIC_FILTER = "skip_dl_warp_cache_manager";
        public static final String WARP_CACHE = "warp_cache_manager";
        public static final String FILTERED_BY_PREDICATE = "filtered_by_predicate";
    }

    public static class WarmingService
    {
        public static final String STARTED = "warm_started";
        public static final String SCHEDULED = "warm_scheduled";
        public static final String FINISHED = "warm_finished";
        public static final String WARM_FAILED = "warm_failed";
        public static final String WARM_ACCOMPLISHED = "warm_accomplished";
        public static final String WARMUP_ELEMENTS_COUNT = "warmup_elements_count";
        public static final String ROW_GROUP_COUNT = "row_group_count";

        public static final String CACHE_WARM_STARTED = "warm_warp_cache_started";
        public static final String CACHE_WARM_ACCOMPLISHED = "warm_warp_cache_accomplished";

        public static final String CACHE_WARM_FAILED = "warm_warp_cache_failed";
        public static final String CACHE_INVALID_TYPE = "warm_warp_cache_invalid_type";
    }

    public static class WarmupDemoter
    {
        public static final String NUMBER_OF_RUNS = "number_of_runs";
        public static final String NUMBER_OF_RUNS_FAIL = "number_of_runs_fail";
    }

    public static class WarmupExportService
    {
        public static final String EXPORT_ROW_GROUP_FINISHED = "export_row_group_finished";
        public static final String EXPORT_ROW_GROUP_ACCOMPLISHED = "export_row_group_accomplished";
        public static final String EXPORT_ROW_GROUP_SCHEDULED = "export_row_group_scheduled";
        public static final String EXPORT_ROW_GROUP_STARTED = "export_row_group_started";
    }

    public static class WarmupImportService
    {
        public static final String IMPORT_ROW_GROUP_COUNT_FAILED = "import_row_group_count_failed";
        public static final String IMPORT_ROW_GROUP_COUNT_ACCOMPLISHED = "import_row_group_count_accomplished";
        public static final String IMPORT_ROW_GROUP_COUNT_STARTED = "import_row_group_count_started";
        public static final String IMPORT_ELEMENTS_STARTED = "import_elements_started";
        public static final String IMPORT_ELEMENTS_FAILED = "import_elements_failed";
        public static final String IMPORT_ELEMENTS_ACCOMPLISHED = "import_elements_accomplished";
    }

    public static class Dictionary
    {
        public static final String DICTIONARY_MAX_EXCEPTION_COUNT = "dictionary_max_exception_count";
        public static final String DICTIONARY_REJECTED_ELEMENTS_COUNT = "dictionary_rejected_elements_count";
        public static final String DICTIONARY_SUCCESS_ELEMENTS_COUNT = "dictionary_success_elements_count";
        public static final String WRITE_DICTIONARIES_COUNT = "write_dictionaries_count";
    }
}
