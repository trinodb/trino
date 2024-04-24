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
package io.trino.plugin.atop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;

import java.util.Map;
import java.util.TimeZone;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class AtopQueryRunner
{
    private AtopQueryRunner() {}

    public static QueryRunner createQueryRunner()
    {
        return createQueryRunner(ImmutableMap.of("atop.executable-path", "/dev/null"), TestingAtopFactory.class);
    }

    public static QueryRunner createQueryRunner(Map<String, String> catalogProperties, Class<? extends AtopFactory> factoryClass)
    {
        Session session = testSessionBuilder()
                .setCatalog("atop")
                .setSchema("default")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID()))
                .build();

        QueryRunner queryRunner = new StandaloneQueryRunner(session);

        try {
            queryRunner.installPlugin(new TestingAtopPlugin(factoryClass));
            queryRunner.createCatalog("atop", "atop", ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("atop.max-history-days", "1").buildOrThrow());
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private record TestingAtopPlugin(Class<? extends AtopFactory> atopFactoryClass)
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new AtopConnectorFactory(atopFactoryClass, AtopQueryRunner.class.getClassLoader()));
        }
    }
}
