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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class StarrocksClient
{
    private final StarrocksConfig config;
    private final StarrocksFEClient feClient;
    private final StarrocksBEClient beClient;

    @Inject
    public StarrocksClient(StarrocksConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.feClient = new StarrocksFEClient(config);
        this.beClient = new StarrocksBEClient(config);
    }

    public StarrocksFEClient getFeClient()
    {
        return feClient;
    }

    public StarrocksBEClient getBeClient()
    {
        return beClient;
    }

    public StarrocksConfig getConfig()
    {
        return config;
    }
}
