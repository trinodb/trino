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

package io.trino.plugin.druid.ingestion;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a Druid index task request body
 */
public class IndexTaskBuilder
{
    private final List<ColumnSpec> columns = new ArrayList<>();
    private String datasource;
    private TimestampSpec timestampSpec;

    public IndexTaskBuilder addColumn(String name, String type)
    {
        columns.add(new ColumnSpec(name, type));
        return this;
    }

    public IndexTaskBuilder setDatasource(String datasource)
    {
        this.datasource = datasource;
        return this;
    }

    public IndexTaskBuilder setTimestampSpec(TimestampSpec timestampSpec)
    {
        this.timestampSpec = timestampSpec;
        return this;
    }

    public String build()
    {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        root.put("type", "index");
        ObjectNode spec = root.putObject("spec");

        ObjectNode dataSchema = spec.putObject("dataSchema");
        dataSchema.put("dataSource", datasource);
        ObjectNode timestamp = dataSchema.putObject("timestampSpec");
        timestamp.put("column", timestampSpec.getColumn());
        timestamp.put("format", timestampSpec.getFormat());
        ArrayNode dimensions = dataSchema.putObject("dimensionsSpec").putArray("dimensions");
        for (ColumnSpec column : columns) {
            ObjectNode dimension = dimensions.addObject();
            dimension.put("name", column.getName());
            dimension.put("type", column.getType());
        }
        ObjectNode granularitySpec = dataSchema.putObject("granularitySpec");
        granularitySpec.put("type", "uniform");
        granularitySpec.putArray("intervals").add("1958-01-01/2028-12-01");
        granularitySpec.put("segmentGranularity", "year");
        granularitySpec.put("queryGranularity", "none");

        ObjectNode ioConfig = spec.putObject("ioConfig");
        ioConfig.put("type", "index");
        ObjectNode inputSource = ioConfig.putObject("inputSource");
        inputSource.put("type", "local");
        inputSource.put("baseDir", "/opt/druid/var/");
        inputSource.put("filter", datasource + ".tsv");
        ObjectNode inputFormat = ioConfig.putObject("inputFormat");
        inputFormat.put("type", "tsv");
        inputFormat.put("findColumnsFromHeader", false);
        ArrayNode inputColumns = inputFormat.putArray("columns");
        inputColumns.add(timestampSpec.getColumn());
        for (ColumnSpec column : columns) {
            inputColumns.add(column.getName());
        }
        ioConfig.put("appendToExisting", false);

        ObjectNode tuningConfig = spec.putObject("tuningConfig");
        tuningConfig.put("type", "index");
        tuningConfig.put("maxRowsPerSegment", 5000000);
        tuningConfig.put("maxRowsInMemory", 250000);
        tuningConfig.putObject("segmentWriteOutMediumFactory").put("type", "offHeapMemory");

        return root.toString();
    }
}
