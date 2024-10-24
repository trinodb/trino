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
package io.trino.plugin.redshift;

import io.trino.operator.OperatorStats;
import io.trino.operator.SplitOperatorInfo;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestRedshiftUnload
        extends AbstractTestQueries
{
    private static final String S3_UNLOAD_ROOT = requiredNonEmptySystemProperty("test.redshift.s3.unload.root");
    private static final String AWS_REGION = requiredNonEmptySystemProperty("test.redshift.aws.region");
    private static final String AWS_ACCESS_KEY = requiredNonEmptySystemProperty("test.redshift.aws.access-key");
    private static final String AWS_SECRET_KEY = requiredNonEmptySystemProperty("test.redshift.aws.secret-key");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .setConnectorProperties(
                        Map.of(
                                "redshift.unload-location", S3_UNLOAD_ROOT,
                                "redshift.unload-iam-role", IAM_ROLE,
                                "s3.region", AWS_REGION,
                                "s3.aws-access-key", AWS_ACCESS_KEY,
                                "s3.aws-secret-key", AWS_SECRET_KEY))
                .setInitialTables(List.of(NATION))
                .build();
    }

    @Test
    void testUnloadEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'redshift.unload_enabled'",
                "VALUES ('redshift.unload_enabled', 'true', 'true', 'boolean', 'Use UNLOAD for reading query results')");
    }

    @Test
    void testUnloadFlow()
    {
        assertQueryStats(
                getSession(),
                "SELECT nationkey, name FROM nation WHERE regionkey = 0",
                queryStats -> {
                    Map<String, String> splitInfo =
                            ((SplitOperatorInfo) queryStats.getOperatorSummaries()
                                    .stream()
                                    .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                                    .collect(onlyElement())
                                    .getInfo())
                                    .getSplitInfo();
                    assertThat(splitInfo.get("path")).matches("%s/.*/0001_part_00.parquet".formatted(S3_UNLOAD_ROOT));
                },
                results -> assertThat(results.getRowCount()).isEqualTo(5));
    }

    @Test
    void testUnloadFlowFallbackToJdbc()
    {
        // Fallback to JDBC as limit clause is not supported by UNLOAD
        assertQueryStats(
                getSession(),
                "SELECT nationkey, name FROM nation WHERE regionkey = 0 LIMIT 2",
                queryStats -> {
                    OperatorStats operatorStats = queryStats.getOperatorSummaries()
                            .stream()
                            .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                            .collect(onlyElement());
                    assertThat(operatorStats.getInfo()).isNull();
                },
                results -> assertThat(results.getRowCount()).isEqualTo(2));
    }
}
