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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.util.List;

public abstract class HudiPageSourceCreator
{
    protected final HudiConfig hudiConfig;
    protected final HdfsEnvironment hdfsEnvironment;
    protected final FileFormatDataSourceStats stats;
    protected final DateTimeZone timeZone;

    public HudiPageSourceCreator(
            HudiConfig hudiConfig,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            DateTimeZone timeZone)
    {
        this.hudiConfig = hudiConfig;
        this.hdfsEnvironment = hdfsEnvironment;
        this.stats = stats;
        this.timeZone = timeZone;
    }

    public abstract ConnectorPageSource createPageSource(
            Configuration configuration,
            ConnectorIdentity identity,
            List<HiveColumnHandle> regularColumns,
            HudiSplit hudiSplit);
}
