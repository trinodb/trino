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

package io.trino.sql.planner;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCDS queries.
 * This class is using Hive connector with mocked in memory thrift metastore with un-partitioned TPCDS tables.
 */
public class TestHiveTpcdsCostBasedPlan
        extends AbstractHiveCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public static final String TPCDS_METADATA_DIR = "/hive_metadata/unpartitioned_tpcds";
    public static final List<String> TPCDS_SQL_FILES = IntStream.range(1, 100)
            .mapToObj(i -> format("q%02d", i))
            .map(queryId -> format("/sql/presto/tpcds/%s.sql", queryId))
            .collect(toImmutableList());

    @Override
    protected String getMetadataDir()
    {
        return TPCDS_METADATA_DIR;
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return TPCDS_SQL_FILES.stream();
    }

    public static void main(String[] args)
    {
        new TestHiveTpcdsCostBasedPlan().generate();
    }
}
