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
package io.prestosql.tests.product.launcher.cli;

import io.airlift.airline.Command;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.env.Environments;

@Command(name = "down", description = "shutdown environment created by Launcher")
public final class EnvironmentDown
        implements Runnable
{
    private static final Logger log = Logger.get(EnvironmentDown.class);

    @Override
    public void run()
    {
        log.info("Pruning old environment(s)");
        Environments.pruneEnvironment();
    }
}
