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
package io.trino.tests.product.launcher;

import com.google.common.base.CaseFormat;
import com.google.common.reflect.ClassPath;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.Environments;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.env.jdk.JdkProvider;
import io.trino.tests.product.launcher.suite.Suite;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.reflect.Modifier.isAbstract;

public final class Configurations
{
    private Configurations() {}

    public static List<Class<? extends EnvironmentProvider>> findEnvironmentsByBasePackage(String packageName)
    {
        try {
            return ClassPath.from(Environments.class.getClassLoader()).getTopLevelClassesRecursive(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> clazz.isAnnotationPresent(TestsEnvironment.class))
                    .map(clazz -> (Class<? extends EnvironmentProvider>) clazz.asSubclass(EnvironmentProvider.class))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Class<? extends EnvironmentConfig>> findConfigsByBasePackage(String packageName)
    {
        try {
            return ClassPath.from(Environments.class.getClassLoader()).getTopLevelClassesRecursive(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> !isAbstract(clazz.getModifiers()))
                    .filter(EnvironmentConfig.class::isAssignableFrom)
                    .map(clazz -> (Class<? extends EnvironmentConfig>) clazz.asSubclass(EnvironmentConfig.class))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Class<? extends JdkProvider>> findJdkProvidersByBasePackage(String packageName)
    {
        try {
            return ClassPath.from(Environments.class.getClassLoader()).getTopLevelClassesRecursive(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> !isAbstract(clazz.getModifiers()))
                    .filter(JdkProvider.class::isAssignableFrom)
                    .map(clazz -> (Class<? extends JdkProvider>) clazz.asSubclass(JdkProvider.class))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Class<? extends Suite>> findSuitesByPackageName(String packageName)
    {
        try {
            return ClassPath.from(Environments.class.getClassLoader()).getTopLevelClassesRecursive(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> !isAbstract(clazz.getModifiers()))
                    .filter(Suite.class::isAssignableFrom)
                    .map(clazz -> (Class<? extends Suite>) clazz.asSubclass(Suite.class))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String nameForEnvironmentClass(Class<? extends EnvironmentProvider> clazz)
    {
        String className = clazz.getSimpleName();
        checkArgument(className.matches("^Env[A-Z].*"), "Name of %s should start with 'Env'", clazz);
        return canonicalEnvironmentName(className);
    }

    public static String nameForConfigClass(Class<? extends EnvironmentConfig> clazz)
    {
        String className = clazz.getSimpleName();
        checkArgument(className.matches("^Config[A-Z].*"), "Name of %s should start with 'Config'", clazz);
        return canonicalConfigName(className);
    }

    public static String nameForJdkProvider(Class<? extends JdkProvider> clazz)
    {
        String className = clazz.getSimpleName();
        checkArgument(className.matches("^[A-Z].*JdkProvider$"), "Name of %s should end with 'JdkProvider'", clazz);
        return canonicalJdkProviderName(className);
    }

    public static String nameForSuiteClass(Class<? extends Suite> clazz)
    {
        String className = clazz.getSimpleName();
        checkArgument(className.matches("^Suite[A-Z0-9].*"), "Name of %s should start with 'Suite'", clazz);
        // For a suite "Suite1", the UPPER_CAMEL to LOWER_HYPHEN conversion won't insert a hyphen after "Suite"
        return "suite-" + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, className.replaceFirst("^Suite", ""));
    }

    public static String canonicalEnvironmentName(String name)
    {
        if (name.matches("^Env[A-Z].*")) {
            name = name.replaceFirst("^Env", "");
        }
        return canonicalName(name);
    }

    public static String canonicalJdkProviderName(String name)
    {
        if (name.matches("^.*?JdkProvider$")) {
            name = name.replaceFirst("JdkProvider$", "");
        }

        return canonicalName(name);
    }

    public static String canonicalConfigName(String name)
    {
        return canonicalName(name);
    }

    /**
     * Converts camel case name to hyphenated. Returns input if the name is already hyphenated.
     */
    private static String canonicalName(String name)
    {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, name)
                .replaceAll("-+", "-");
    }
}
