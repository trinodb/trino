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
package io.trino.exchange;

import io.airlift.configuration.Config;

import java.io.File;

public class ExchangeManagerConfig
{
    private File exchangeManagerConfigFile = new File("etc/exchange-manager.properties");

    public File getExchangeManagerConfigFile()
    {
        return exchangeManagerConfigFile;
    }

    @Config("exchange-manager.config-file")
    public ExchangeManagerConfig setExchangeManagerConfigFile(String exchangeManagerConfigFile)
    {
        this.exchangeManagerConfigFile = new File(exchangeManagerConfigFile);
        return this;
    }
}
