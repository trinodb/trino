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

import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.env.Environments;
import picocli.CommandLine.Option;

import static picocli.CommandLine.Command;

@Command(
        name = "down",
        description = "Shutdown environment created by launcher",
        usageHelpAutoWidth = true)
public final class EnvironmentDown
        implements Runnable
{
    private static final Logger log = Logger.get(EnvironmentDown.class);

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Override
    public void run()
    {
        log.info("Pruning old environment(s)");
        Environments.pruneEnvironment();
    }
}
