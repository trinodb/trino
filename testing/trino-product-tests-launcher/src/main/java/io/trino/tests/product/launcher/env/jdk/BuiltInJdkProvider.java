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
package io.trino.tests.product.launcher.env.jdk;

import io.trino.tests.product.launcher.env.DockerContainer;

import static io.trino.tests.product.launcher.Configurations.nameForJdkProvider;

public class BuiltInJdkProvider
        implements JdkProvider
{
    public static final String BUILT_IN_NAME = nameForJdkProvider(BuiltInJdkProvider.class);

    @Override
    public DockerContainer applyTo(DockerContainer container)
    {
        return container;
    }

    @Override
    public String getJavaHome()
    {
        // This is provided by docker image
        return "/usr/lib/jvm/zulu-17";
    }

    @Override
    public String getDescription()
    {
        return "JDK provider by base image";
    }
}
