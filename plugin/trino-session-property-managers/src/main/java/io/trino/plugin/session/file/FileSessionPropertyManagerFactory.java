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
package io.trino.plugin.session.file;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.resourcegroups.SessionPropertyConfigurationManagerContext;
import io.trino.spi.session.SessionPropertyConfigurationManager;
import io.trino.spi.session.SessionPropertyConfigurationManagerFactory;

import java.util.Map;

public class FileSessionPropertyManagerFactory
        implements SessionPropertyConfigurationManagerFactory
{
    @Override
    public String getName()
    {
        return "file";
    }

    @Override
    public SessionPropertyConfigurationManager create(Map<String, String> config, SessionPropertyConfigurationManagerContext context)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.session." + getName(),
                new JsonModule(),
                new FileSessionPropertyManagerModule());

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(FileSessionPropertyManager.class);
    }
}
