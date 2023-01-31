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
    private boolean csvNativeReaderEnabled = true;
    private boolean csvNativeWriterEnabled = true;

    public boolean isCsvNativeReaderEnabled()
    {
        return csvNativeReaderEnabled;
    }

    @Config("csv.native-reader.enabled")
    @ConfigDescription("Use native CSV reader")
    public HiveFormatsConfig setCsvNativeReaderEnabled(boolean csvNativeReaderEnabled)
    {
        this.csvNativeReaderEnabled = csvNativeReaderEnabled;
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
}
