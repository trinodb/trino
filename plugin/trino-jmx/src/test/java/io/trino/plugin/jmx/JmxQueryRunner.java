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
package io.trino.plugin.jmx;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class JmxQueryRunner
{
    private JmxQueryRunner() {}

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static QueryRunner createJmxQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("jmx")
                .setSchema(JMX_SCHEMA_NAME)
                .build();
    }
}
