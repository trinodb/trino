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
package io.trino.tests.product.launcher.suite;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.tests.product.launcher.Configurations.findSuitesByPackageName;
import static io.trino.tests.product.launcher.Configurations.nameForSuiteClass;
import static java.util.Objects.requireNonNull;

public final class SuiteModule
        implements Module
{
    public static final String BASE_SUITES_PACKAGE = "io.trino.tests.product.launcher.suite";
    private final Module additionalSuites;

    public SuiteModule(Module additionalSuites)
    {
        this.additionalSuites = requireNonNull(additionalSuites, "additionalSuites is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(SuiteFactory.class).in(SINGLETON);

        MapBinder<String, Suite> suites = newMapBinder(binder, String.class, Suite.class);
        findSuitesByPackageName(BASE_SUITES_PACKAGE).forEach(clazz -> suites.addBinding(nameForSuiteClass(clazz)).to(clazz).in(SINGLETON));

        binder.install(additionalSuites);
    }
}
