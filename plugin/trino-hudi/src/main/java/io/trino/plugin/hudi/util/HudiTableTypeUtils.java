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
package io.trino.plugin.hudi.util;

import io.trino.plugin.hudi.HudiErrorCode;
import io.trino.spi.TrinoException;
import org.apache.hudi.common.model.HoodieTableType;

public class HudiTableTypeUtils
{
    private static final String HUDI_PARQUET_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    private static final String HUDI_PARQUET_REALTIME_INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    private static final String HUDI_INPUT_FORMAT = "com.uber.hoodie.hadoop.HoodieInputFormat";
    private static final String HUDI_REALTIME_INPUT_FORMAT = "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat";

    private HudiTableTypeUtils()
    {
    }

    public static HoodieTableType fromInputFormat(String inputFormat)
    {
        switch (inputFormat) {
            case HUDI_PARQUET_INPUT_FORMAT:
            case HUDI_INPUT_FORMAT:
                return HoodieTableType.COPY_ON_WRITE;
            case HUDI_PARQUET_REALTIME_INPUT_FORMAT:
            case HUDI_REALTIME_INPUT_FORMAT:
                return HoodieTableType.MERGE_ON_READ;
            default:
                throw new TrinoException(HudiErrorCode.HUDI_UNSUPPORTED_TABLE_TYPE, "Table has an unsupported input format: " + inputFormat);
        }
    }
}
