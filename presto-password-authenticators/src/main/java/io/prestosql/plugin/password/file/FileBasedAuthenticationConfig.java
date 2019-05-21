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
package io.prestosql.plugin.password.file;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class FileBasedAuthenticationConfig
{
    private String configFile;
    private Duration refreshInterval = new Duration(5, TimeUnit.SECONDS);
    private int bcryptMinCost = 8;
    private int pbkdf2MinIterations = 1000;
    private int authTokenCacheStoreMaxSize = 1000; //maximum size of the LRU cache for storing auth token

    @NotNull
    public String getConfigFile()
    {
        return configFile;
    }

    @Config("file.config-file")
    @ConfigDescription("Location of the file that provides user names and passwords")
    public FileBasedAuthenticationConfig setConfigFile(String configFileName)
    {
        configFile = configFileName;
        return this;
    }

    public Duration getRefreshPeriod()
    {
        return refreshInterval;
    }

    @Config("file.refresh-period")
    @ConfigDescription("Reloading period of the file that provides user names and passwords")
    public FileBasedAuthenticationConfig setRefreshPeriod(Duration refreshPeriodValue)
    {
        refreshInterval = refreshPeriodValue;
        return this;
    }

    @NotNull
    public int getBcryptMinCost()
    {
        return this.bcryptMinCost;
    }

    @Config("file.bcrypt.min-cost")
    @ConfigDescription("Min cost allowance for the bcrypt passwords")
    public FileBasedAuthenticationConfig setBcryptMinCost(int bcryptMinCost)
    {
        this.bcryptMinCost = bcryptMinCost;
        return this;
    }

    @NotNull
    public int getPbkdf2MinIterations()
    {
        return this.pbkdf2MinIterations;
    }

    @Config("file.pbkdf2.min-iterations")
    @ConfigDescription("Min iteration allowance for PBKDF2 passwords")
    public FileBasedAuthenticationConfig setPbkdf2MinIterations(int pbkdf2MinIterations)
    {
        this.pbkdf2MinIterations = pbkdf2MinIterations;
        return this;
    }

    public int getAuthTokenCacheStoreMaxSize()
    {
        return authTokenCacheStoreMaxSize;
    }

    @Config("file.auth-token-cache-max-size")
    @ConfigDescription("Max cache size for storing authenticated passwords")
    public FileBasedAuthenticationConfig setAuthTokenCacheStoreMaxSize(int authTokenCacheStoreMaxSize)
    {
        this.authTokenCacheStoreMaxSize = authTokenCacheStoreMaxSize;
        return this;
    }
}
