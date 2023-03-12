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
package io.trino.plugin.hive.parquet;

import com.google.common.io.Resources;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReadingTimeLogicalAnnotation
{
    @Test
    public void testReadingTimeLogicalAnnotationAsBigInt()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("parquet_file_with_time_logical_annotation").toURI());
        try (DistributedQueryRunner queryRunner = HiveQueryRunner.builder().build();
                QueryAssertions assertions = new QueryAssertions(queryRunner)) {
            queryRunner.execute(format("""
                            CREATE TABLE table_with_time_logical_annotation (
                                "opens" row(member0 bigint, member_1 varchar))
                            WITH (
                                external_location = '%s',
                                format = 'PARQUET')
                            """,
                    parquetFile.getAbsolutePath()));

            assertThat(assertions.query("SELECT opens.member0 FROM table_with_time_logical_annotation GROUP BY 1 ORDER BY 1 LIMIT 5"))
                    .matches(resultBuilder(queryRunner.getDefaultSession(), BIGINT)
                            .row(0L)
                            .row(21600000000L)
                            .row(25200000000L)
                            .row(28800000000L)
                            .row(32400000000L)
                            .build());
            queryRunner.execute("DROP TABLE table_with_time_logical_annotation");
        }
    }
}
