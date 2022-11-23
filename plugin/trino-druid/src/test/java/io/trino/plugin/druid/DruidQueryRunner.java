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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.tpch.TpchTable;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DruidQueryRunner
{
    private static final Logger log = Logger.get(DruidQueryRunner.class);

    private static final String SCHEMA = "druid";

    private DruidQueryRunner() {}

    public static DistributedQueryRunner createDruidQueryRunnerTpch(TestingDruidServer testingDruidServer, Map<String, String> extraProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> connectorProperties = new HashMap<>();
            connectorProperties.putIfAbsent("connection-url", testingDruidServer.getJdbcUrl());
            queryRunner.installPlugin(new DruidJdbcPlugin());
            queryRunner.createCatalog("druid", "druid", connectorProperties);

            log.info("Loading data from druid.%s...", SCHEMA);
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                long start = System.nanoTime();
                log.info("Running import for %s", table.getTableName());
                MaterializedResult rows = queryRunner.execute(DruidTpchTables.getSelectQuery(table.getTableName()));
                copyAndIngestTpchData(rows, testingDruidServer, table.getTableName());
                log.info("Imported %s rows for %s in %s", rows.getRowCount(), table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
            }
            log.info("Loading from druid.%s complete in %s", SCHEMA, nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void copyAndIngestTpchData(MaterializedResult rows, TestingDruidServer testingDruidServer, String druidDatasource)
            throws IOException, InterruptedException
    {
        String tsvFileLocation = format("%s/%s.tsv", testingDruidServer.getHostWorkingDirectory(), druidDatasource);
        writeDataAsTsv(rows, tsvFileLocation);
        testingDruidServer.ingestData(
                druidDatasource,
                Resources.toString(
                        getResource(getIngestionSpecFileName(druidDatasource)),
                        Charset.defaultCharset()),
                tsvFileLocation);
    }

    private static String getIngestionSpecFileName(String datasource)
    {
        return format("druid-tpch-ingest-%s.json", datasource);
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("druid")
                .setSchema(SCHEMA)
                .build();
    }

    private static void writeDataAsTsv(MaterializedResult rows, String dataFile)
            throws IOException
    {
        File file = new File(dataFile);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file, UTF_8))) {
            for (MaterializedRow row : rows.getMaterializedRows()) {
                bw.write(convertToTSV(row.getFields()));
                bw.newLine();
            }
        }
    }

    private static String convertToTSV(List<Object> data)
    {
        return data.stream()
                .map(String::valueOf)
                .collect(Collectors.joining("\t"));
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createDruidQueryRunnerTpch(
                new TestingDruidServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableList.of(ORDERS, LINE_ITEM, NATION, REGION, PART, CUSTOMER));

        Logger log = Logger.get(DruidQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
