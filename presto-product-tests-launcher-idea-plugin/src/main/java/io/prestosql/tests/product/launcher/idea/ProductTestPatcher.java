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
package io.prestosql.tests.product.launcher.idea;

import com.intellij.execution.Executor;
import com.intellij.execution.configurations.JavaParameters;
import com.intellij.execution.configurations.RunProfile;
import com.intellij.execution.runners.JavaProgramPatcher;
import com.theoryinpractice.testng.configuration.TestNGConfiguration;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBeforeLast;

/**
 * During product test execution, replaces {@code java ...} process execution with corresponding {@code ...Launcher env run -- java ...}.
 */
/*
 * Design note: better method would be extending TestNG launch configurations, unfortunately it's much, much more complex than JavaProgramPatcher approach.
 */
public class ProductTestPatcher
        extends JavaProgramPatcher
{
    private static final String thisJarPath;
    private final ProductTestJvmPatcher jvmPatcher = new ProductTestJvmPatcher();

    static {
        try {
            String thisJarUri = URLDecoder.decode(ProductTestPatcher.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString(), UTF_8);
            thisJarPath = substringBeforeLast(substringAfter(substringAfter(thisJarUri, ":"), ":"), "!");
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void patchJavaParameters(Executor executor, RunProfile configuration, JavaParameters javaParameters)
    {
        if (configuration instanceof TestNGConfiguration &&
                Stream.of(((TestNGConfiguration) configuration).getModules()).anyMatch(m -> m.getName().endsWith("-product-tests"))) {
            jvmPatcher.patch(javaParameters);
            checkState(new File(thisJarPath).isFile(), thisJarPath + " doesn't exist. Maybe you reinstalled the plugin but forgot to restart IntelliJ?");
            javaParameters.getClassPath().add(thisJarPath); // necessary for DockerRemoteTestNGStarter
        }
    }
}
