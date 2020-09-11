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

import com.google.common.io.Resources;
import com.intellij.execution.configurations.JavaParameters;
import com.intellij.openapi.projectRoots.Sdk;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.Files.asByteSink;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Resources.getResource;
import static java.util.Objects.requireNonNull;

/**
 * JVM options are composed by IntelliJ at the very end, it's not possible to patch them with {@link JavaParameters} alone,
 * so we have to use {@link ProductTestJvmPatcher#patch(JavaParameters)}, which patches whole JVM.
 */
class ProductTestJvmPatcher
{
    private static final File fakeJdkHome;

    static {
        try {
            //noinspection UnstableApiUsage
            fakeJdkHome = createTempDir();
            fakeJdkHome.deleteOnExit(); // we don't have close() or anything like that in IntelliJ APIs, so have to delete on exit

            File fakeJavaExe = new File(fakeJdkHome, "bin/java");
            verify(fakeJavaExe.getParentFile().mkdirs());
            fakeJavaExe.getParentFile().deleteOnExit();
            //noinspection UnstableApiUsage
            Resources.asByteSource(getResource(ProductTestJvmPatcher.class, "patched-java")).copyTo(asByteSink(fakeJavaExe));
            fakeJavaExe.deleteOnExit();
            verify(fakeJavaExe.setExecutable(true, true));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void patch(JavaParameters javaParameters)
    {
        javaParameters.addEnv("_PT_JAVA_EXE", requireNonNull(javaParameters.getJdk()).getHomePath() + "/bin/java");
        javaParameters.setJdk(wrapSdk(requireNonNull(javaParameters.getJdk())));
    }

    private Sdk wrapSdk(Sdk jdk)
    {
        return createProxy(jdk, (proxy, method, args) -> {
            if (method.getName().equals("getHomePath") && method.getParameterCount() == 0) {
                return fakeJdkHome.getPath();
            }
            else {
                return method.invoke(jdk, args);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <T> T createProxy(T object, InvocationHandler handler)
    {
        return (T) Proxy.newProxyInstance(object.getClass().getClassLoader(), object.getClass().getInterfaces(), handler);
    }
}
