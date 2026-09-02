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
package io.trino.plugin.deltalake;

import com.google.common.io.Resources;
import io.trino.plugin.hive.BaseTestParquetPageSkipping;
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;

public class TestDeltaLakeParquetPageSkipping
        extends BaseTestParquetPageSkipping
{
    private Path catalogDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalogDir = Files.createTempDirectory("delta-page-skipping");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("parquet.use-column-index", "true")
                .addDeltaProperty("parquet.max-buffer-size", "1MB")
                .build();
    }

    @Override
    protected String createTableWithDataFile(String tableNamePrefix, String columnsDefinition, String resourceFileName)
            throws IOException
    {
        String tableName = tableName(tableNamePrefix);
        Path tableLocation = catalogDir.resolve(tableName);
        assertUpdate(format("CREATE TABLE %s %s WITH (location = '%s')", tableName, columnsDefinition, tableLocation.toUri()));

        Path dataFile = tableLocation.resolve("data.parquet");
        try (OutputStream output = Files.newOutputStream(dataFile)) {
            Resources.copy(Resources.getResource(resourceFileName), output);
        }
        String addAction = format(
                "{\"add\":{\"path\":\"data.parquet\",\"partitionValues\":{},\"size\":%d,\"modificationTime\":%d,\"dataChange\":true}}",
                Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis());
        Files.writeString(tableLocation.resolve("_delta_log").resolve("00000000000000000001.json"), addAction + "\n");
        return tableName;
    }

    @Override
    protected String timestampMillisType()
    {
        return "timestamp(3) with time zone";
    }
}
