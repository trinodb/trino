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
package io.trino.hdfs.cos;

import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;

import java.io.File;

public class HiveCosServiceConfig
{
    private File serviceConfig;

    @Deprecated(forRemoval = true, since = "470")
    @FileExists
    public File getServiceConfig()
    {
        return serviceConfig;
    }

    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.cos.service-config")
    public HiveCosServiceConfig setServiceConfig(File serviceConfig)
    {
        this.serviceConfig = serviceConfig;
        return this;
    }
}
