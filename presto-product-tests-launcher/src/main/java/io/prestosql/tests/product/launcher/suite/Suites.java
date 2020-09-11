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
package io.prestosql.tests.product.launcher.suite;

import com.google.common.base.CaseFormat;
import com.google.common.reflect.ClassPath;
import io.prestosql.tests.product.launcher.env.Environments;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.reflect.Modifier.isAbstract;

public class Suites
{
    private Suites() {}

    public static List<Class<? extends Suite>> findSuitesByPackageName(String packageName)
    {
        try {
            return ClassPath.from(Environments.class.getClassLoader()).getTopLevelClassesRecursive(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> !isAbstract(clazz.getModifiers()))
                    .filter(clazz -> Suite.class.isAssignableFrom(clazz))
                    .map(clazz -> (Class<? extends Suite>) clazz.asSubclass(Suite.class))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String nameForSuiteClass(Class<? extends Suite> clazz)
    {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, clazz.getSimpleName().replace("Suite", "Suite-")).replace("--", "-");
    }
}
