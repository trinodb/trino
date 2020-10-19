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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.prestosql.tests.product.launcher.env.DockerContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.shaded.org.apache.commons.lang.ArrayUtils.indexOf;

class EnvironmentRunProductTestConfigurer
{
    static final String INTELLIJ_TESTNG_STARTER_MAIN_CLASS = "com.intellij.rt.testng.RemoteTestNGStarter";

    private final EnvironmentRun environmentRun;
    private final TestRun.Execution testRunExecution;

    @Inject
    EnvironmentRunProductTestConfigurer(EnvironmentRun environmentRun, TestRun.Execution testRunExecution)
    {
        this.environmentRun = environmentRun;
        this.testRunExecution = testRunExecution;
    }

    void configureIfNeeded(CreateContainerCmd container)
    {
        if (environmentRun.command.contains(INTELLIJ_TESTNG_STARTER_MAIN_CLASS)) {
            DockerContainer srcContainer = testRunExecution.getEnvironment().getContainer("tests");
            container.withName("ptl-tests");
            Stream<Bind> binds = Streams.concat(Stream.of(requireNonNull(container.getHostConfig()).getBinds()), srcContainer.getBinds().stream());
            container.getHostConfig().withBinds(binds.toArray(Bind[]::new));

            List<String> extraJvmOpts = new ArrayList<>();
            String[] srcArgs = srcContainer.getCommandParts();
            for (int i = 1; i < srcArgs.length && !srcArgs[i].equals("-jar"); ++i) {
                extraJvmOpts.add(srcArgs[i]);
            }
            extraJvmOpts.add("-Dtempto.configurations=" + srcArgs[indexOf(srcArgs, "--config") + 1]);
            container.withCmd(Stream.of(requireNonNull(container.getCmd()))
                    .flatMap(arg -> {
                        if (!arg.equals(INTELLIJ_TESTNG_STARTER_MAIN_CLASS)) {
                            return Stream.of(arg);
                        }
                        return Streams.concat(extraJvmOpts.stream(), Stream.of("com.intellij.rt.testng.DockerRemoteTestNGStarter"));
                    })
                    .collect(toImmutableList()));
        }
    }
}
