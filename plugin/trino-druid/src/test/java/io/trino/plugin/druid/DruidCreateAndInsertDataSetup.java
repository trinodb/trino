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

import io.airlift.log.Logger;
import io.trino.plugin.druid.ingestion.IndexTaskBuilder;
import io.trino.plugin.druid.ingestion.TimestampSpec;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DruidCreateAndInsertDataSetup
        implements DataSetup
{
    private static final Logger log = Logger.get(DruidCreateAndInsertDataSetup.class);
    private final TestingDruidServer druidServer;
    private final String dataSourceNamePrefix;

    public DruidCreateAndInsertDataSetup(TestingDruidServer druidServer, String dataSourceNamePrefix)
    {
        this.druidServer = druidServer;
        this.dataSourceNamePrefix = dataSourceNamePrefix;
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        DruidTable testTable = new DruidTable(this.dataSourceNamePrefix);
        try {
            ingestData(testTable, inputs);
        }
        catch (Exception e) {
            log.error(e);
        }
        return testTable;
    }

    private void ingestData(DruidTable testTable, List<ColumnSetup> inputs)
            throws Exception
    {
        IndexTaskBuilder builder = new IndexTaskBuilder();
        builder.setDatasource(testTable.getName());
        TimestampSpec timestampSpec = getTimestampSpec(inputs);
        builder.setTimestampSpec(timestampSpec);

        List<ColumnSetup> normalInputs = inputs.stream().filter(input -> !isTimestampDimension(input)).collect(Collectors.toList());
        for (int index = 0; index < inputs.size() - 1; index++) {
            builder.addColumn(format("col_%s", index), normalInputs.get(index).getDeclaredType().orElse("string"));
        }

        String dataFilePath = format("%s/%s.tsv", druidServer.getHostWorkingDirectory(), testTable.getName());
        writeTsvFile(dataFilePath, inputs);

        log.debug(builder.build());
        this.druidServer.ingestData(testTable.getName(), builder.build(), dataFilePath);
    }

    private TimestampSpec getTimestampSpec(List<ColumnSetup> inputs)
    {
        List<ColumnSetup> timestampInputs = inputs.stream().filter(this::isTimestampDimension).collect(Collectors.toList());

        if (timestampInputs.size() > 1) {
            throw new UnsupportedOperationException("Druid only allows one timestamp field");
        }

        return new TimestampSpec("dummy_druid_ts", "auto");
    }

    private boolean isTimestampDimension(ColumnSetup input)
    {
        if (input.getDeclaredType().isEmpty()) {
            return false;
        }
        String type = input.getDeclaredType().get();

        // TODO: support more types
        return type.startsWith("timestamp");
    }

    private void writeTsvFile(String dataFilePath, List<ColumnSetup> inputs)
            throws IOException
    {
        File file = new File(dataFilePath);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, UTF_8))) {
            writer.write(inputs.stream().map(ColumnSetup::getInputLiteral).collect(Collectors.joining("\t")));
        }
    }
}
