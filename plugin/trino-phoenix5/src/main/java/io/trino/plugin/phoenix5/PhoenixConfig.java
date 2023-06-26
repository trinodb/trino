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
package io.trino.plugin.phoenix5;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class PhoenixConfig
{
    static final int MAX_ALLOWED_SCANS_PER_SPLIT = 1000;

    private String connectionUrl;
    private List<String> resourceConfigFiles = ImmutableList.of();

    /*
     * By default group at most 20 HBase scans into a single Split.
     * There is at least one Split per HBase region, HBase's default region size is 20GB
     * and Phoenix' default scan chunk size is 300MB.
     * Any value between 10 and perhaps 30 is a good default. 20 is a good compromise allowing
     * 3-4 parallel scans per region with all default settings.
     * A large value here makes sense when the Guidepost-width in Phoenix has been reduced.
     */
    private int maxScansPerSplit = 20;
    private boolean reuseConnection = true;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("phoenix.connection-url")
    public PhoenixConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public List<@FileExists String> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("phoenix.config.resources")
    public PhoenixConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(files);
        return this;
    }

    @Min(1)
    @Max(MAX_ALLOWED_SCANS_PER_SPLIT)
    public int getMaxScansPerSplit()
    {
        return maxScansPerSplit;
    }

    @Config("phoenix.max-scans-per-split")
    @ConfigDescription("Maximum number of HBase scans that will be performed in a single split.")
    public PhoenixConfig setMaxScansPerSplit(int scansPerSplit)
    {
        this.maxScansPerSplit = scansPerSplit;
        return this;
    }

    public boolean isReuseConnection()
    {
        return reuseConnection;
    }

    @Config("query.reuse-connection")
    @ConfigDescription("Enables reusing JDBC connection within single Trino query to run metadata queries from Coordinator to remote service")
    public PhoenixConfig setReuseConnection(boolean reuseConnection)
    {
        this.reuseConnection = reuseConnection;
        return this;
    }
}
