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
package io.trino.plugin.hive;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HiveFormatsConfig
{
    private boolean avroFileNativeWriterEnabled = true;
    private boolean csvNativeWriterEnabled = true;
    private boolean jsonNativeWriterEnabled = true;
    private boolean openXJsonNativeWriterEnabled = true;
    private boolean textFileNativeWriterEnabled = true;
    private boolean sequenceFileNativeWriterEnabled = true;

    public boolean isAvroFileNativeWriterEnabled()
    {
        return avroFileNativeWriterEnabled;
    }

    @Config("avro.native-writer.enabled")
    @ConfigDescription("Use native Avro file writer")
    public HiveFormatsConfig setAvroFileNativeWriterEnabled(boolean avroFileNativeWriterEnabled)
    {
        this.avroFileNativeWriterEnabled = avroFileNativeWriterEnabled;
        return this;
    }

    public boolean isCsvNativeWriterEnabled()
    {
        return csvNativeWriterEnabled;
    }

    @Config("csv.native-writer.enabled")
    @ConfigDescription("Use native CSV writer")
    public HiveFormatsConfig setCsvNativeWriterEnabled(boolean csvNativeWriterEnabled)
    {
        this.csvNativeWriterEnabled = csvNativeWriterEnabled;
        return this;
    }

    public boolean isJsonNativeWriterEnabled()
    {
        return jsonNativeWriterEnabled;
    }

    @Config("json.native-writer.enabled")
    @ConfigDescription("Use native JSON writer")
    public HiveFormatsConfig setJsonNativeWriterEnabled(boolean jsonNativeWriterEnabled)
    {
        this.jsonNativeWriterEnabled = jsonNativeWriterEnabled;
        return this;
    }

    public boolean isOpenXJsonNativeWriterEnabled()
    {
        return openXJsonNativeWriterEnabled;
    }

    @Config("openx-json.native-writer.enabled")
    @ConfigDescription("Use native OpenXJson writer")
    public HiveFormatsConfig setOpenXJsonNativeWriterEnabled(boolean openXJsonNativeWriterEnabled)
    {
        this.openXJsonNativeWriterEnabled = openXJsonNativeWriterEnabled;
        return this;
    }

    public boolean isTextFileNativeWriterEnabled()
    {
        return textFileNativeWriterEnabled;
    }

    @Config("text-file.native-writer.enabled")
    @ConfigDescription("Use native text file writer")
    public HiveFormatsConfig setTextFileNativeWriterEnabled(boolean textFileNativeWriterEnabled)
    {
        this.textFileNativeWriterEnabled = textFileNativeWriterEnabled;
        return this;
    }

    public boolean isSequenceFileNativeWriterEnabled()
    {
        return sequenceFileNativeWriterEnabled;
    }

    @Config("sequence-file.native-writer.enabled")
    @ConfigDescription("Use native sequence file writer")
    public HiveFormatsConfig setSequenceFileNativeWriterEnabled(boolean sequenceFileNativeWriterEnabled)
    {
        this.sequenceFileNativeWriterEnabled = sequenceFileNativeWriterEnabled;
        return this;
    }
}
