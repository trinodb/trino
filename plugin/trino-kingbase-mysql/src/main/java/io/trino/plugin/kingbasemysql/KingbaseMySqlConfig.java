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
package io.trino.plugin.kingbasemysql;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * 连接 KingbaseES（MySQL 兼容模式）时的连接相关配置。
 */
public class KingbaseMySqlConfig
{
    /** 连接超时，对应 KES JDBC connectTimeout，单位秒。 */
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("mysql.connection-timeout")
    @ConfigDescription("Connection timeout. Maps to KES JDBC connectTimeout (seconds)")
    public KingbaseMySqlConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }
}
