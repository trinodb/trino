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
package io.prestosql.plugin.druid;

import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class DruidQueryRunner
{
    private DruidQueryRunner() {}

    public static QueryRunner createDruidQueryRunnerTpch(TestingDruidServer testingDruidServer)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).setNodeCount(3).build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> connectorProperties = new HashMap<>();
            connectorProperties.putIfAbsent("connection-url", testingDruidServer.getJdbcUrl());
            queryRunner.installPlugin(new DruidJdbcPlugin());
            queryRunner.createCatalog("druid", "druid", connectorProperties);
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
        String tsvFileLocation = String.format("%s/%s.tsv", testingDruidServer.getHostWorkingDirectory(), druidDatasource);
        writeDataAsTsv(rows, tsvFileLocation);
        testingDruidServer.ingestData(druidDatasource, getIngestionSpecFileName(druidDatasource), tsvFileLocation);
    }

    private static String getIngestionSpecFileName(String datasource)
    {
        return String.format("druid-tpch-ingest-%s.json", datasource);
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("druid")
                .setSchema("druid")
                .build();
    }

    private static void writeDataAsTsv(MaterializedResult rows, String dataFile)
            throws IOException
    {
        File file = new File(dataFile);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
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
}
