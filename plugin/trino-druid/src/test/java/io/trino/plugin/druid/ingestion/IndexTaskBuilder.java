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

import com.google.common.io.Resources;
import freemarker.template.Template;
import io.airlift.log.Logger;

import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;

import static com.google.common.io.Resources.getResource;

/**
 * This builder is used to build Druid index task request body
 * and follows same format as files inside resources directory
 */

public class IndexTaskBuilder
{
    private static final Logger log = Logger.get(IndexTaskBuilder.class);
    private final ArrayList<ColumnSpec> columns;
    private String datasource;
    private TimestampSpec timestampSpec;

    public IndexTaskBuilder()
    {
        this.columns = new ArrayList<>();
    }

    public IndexTaskBuilder addColumn(String name, String type)
    {
        columns.add(new ColumnSpec(name, type));
        return this;
    }

    public String getDatasource()
    {
        return datasource;
    }

    public IndexTaskBuilder setDatasource(String datasource)
    {
        this.datasource = datasource;
        return this;
    }

    public TimestampSpec getTimestampSpec()
    {
        return timestampSpec;
    }

    public IndexTaskBuilder setTimestampSpec(TimestampSpec timestampSpec)
    {
        this.timestampSpec = timestampSpec;
        return this;
    }

    public ArrayList<ColumnSpec> getColumns()
    {
        return columns;
    }

    public String build()
    {
        String tplContent;
        try {
            String tmpFile = "ingestion-index.tpl";
            tplContent = Resources.toString(getResource(tmpFile), Charset.defaultCharset());
            Template template = new Template("ingestion-task", new StringReader(tplContent));
            StringWriter writer = new StringWriter();
            template.process(this, writer);
            return writer.toString();
        }
        catch (Exception ex) {
            log.error(ex);
        }
        return "";
    }
}
