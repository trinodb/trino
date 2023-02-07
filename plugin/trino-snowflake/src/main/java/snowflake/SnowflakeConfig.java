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
package snowflake;

import io.airlift.configuration.Config;

public class SnowflakeConfig
{
    /*public final static String SNOWFLAKE_ACCOUNT = "snowflake.account";
    public final static String SNOWFLAKE_USER = "snowflake.user";
    public final static String SNOWFLAKE_PASSWORD = "snowflake.password";*/
    public final static String SNOWFLAKE_WAREHOUSE = "snowflake.warehouse";
    public final static String SNOWFLAKE_CATALOG = "snowflake.catalog";
    public final static String SNOWFLAKE_STAGE_NAME = "snowflake.stage.name";
    public final static String SNOWFLAKE_STAGE_LOCATION = "snowflake.stage.location";
    private String warehouse = null;
    private String stageName = null;
    private String catalog = null;
    private String stageLocation = null;
    private String account = null;
    private String password = null;
    private String user = null;

    public String getUser()
    {
        return user;
    }

    public String getWarehouse()
    {
        return warehouse;
    }

    @Config(SNOWFLAKE_WAREHOUSE)
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public String getAccount()
    {
        return account;
    }
/*
    @Config(SNOWFLAKE_ACCOUNT)
    public SnowflakeConfig setAccount(String account)
    {
        this.account = account;
        return this;
    }
    @Config(SNOWFLAKE_USER)
    public SnowflakeConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config(SNOWFLAKE_PASSWORD)
    public SnowflakeConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }*/

    public String getStageName()
    {
        return stageName;
    }

    @Config(SNOWFLAKE_STAGE_NAME)
    public SnowflakeConfig setStageName(String stageName)
    {
        this.stageName = stageName;
        return this;
    }

    public String getStageLocation()
    {
        return stageLocation;
    }

    @Config(SNOWFLAKE_STAGE_LOCATION)
    public SnowflakeConfig setStageLocation(String stageLocation)
    {
        this.stageLocation = stageLocation;
        return this;
    }

    public String getCatalog()
    {
        return catalog;
    }

    @Config(SNOWFLAKE_CATALOG)
    public SnowflakeConfig setCatalog(String catalog)
    {
        this.catalog = catalog;
        return this;
    }
}
