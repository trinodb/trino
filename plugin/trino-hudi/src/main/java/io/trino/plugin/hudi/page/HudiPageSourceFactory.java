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

package io.trino.plugin.hudi.page;

import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hudi.HudiConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.exception.HoodieIOException;
import org.joda.time.DateTimeZone;

import java.util.Map;

import static java.lang.String.format;

public final class HudiPageSourceFactory
{
    private HudiPageSourceFactory() {}

    public static HudiPageSourceCreator get(
            HoodieFileFormat baseFileFormat,
            HudiConfig hudiConfig,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            DateTimeZone timeZone,
            Map<String, Object> context)
    {
        switch (baseFileFormat) {
            case PARQUET:
                return new HudiParquetPageSourceCreator(hudiConfig, hdfsEnvironment, stats, timeZone, context);
            default:
                throw new HoodieIOException(format("Base file format %s is not supported yet", baseFileFormat));
        }
    }
}
