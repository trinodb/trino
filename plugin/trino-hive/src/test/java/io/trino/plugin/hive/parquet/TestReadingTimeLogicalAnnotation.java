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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.URL;
import java.util.UUID;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReadingTimeLogicalAnnotation
{
    @Test
    public void testReadingTimeLogicalAnnotationAsBigInt()
            throws Exception
    {
        try (QueryRunner queryRunner = HiveQueryRunner.builder().build();
                QueryAssertions assertions = new QueryAssertions(queryRunner)) {
            URL resourceLocation = Resources.getResource("parquet_file_with_time_logical_annotation/time-micros.parquet");

            TrinoFileSystem fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                    .create(ConnectorIdentity.ofUser("test"));

            Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
            fileSystem.createDirectory(tempDir);
            Location dataFile = tempDir.appendPath("data.parquet");
            try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
                Resources.copy(resourceLocation, out);
            }

            queryRunner.execute("""
                            CREATE TABLE table_with_time_logical_annotation (
                                "opens" row(member0 bigint, member_1 varchar))
                            WITH (
                                external_location = '%s',
                                format = 'PARQUET')
                            """.formatted(dataFile.parentDirectory()));

            assertThat(assertions.query("SELECT opens.member0 FROM table_with_time_logical_annotation GROUP BY 1 ORDER BY 1 LIMIT 5"))
                    .result().matches(resultBuilder(queryRunner.getDefaultSession(), BIGINT)
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
