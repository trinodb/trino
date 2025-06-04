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
package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;

import java.util.List;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.LDAP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OpenLdapReferral
        implements EnvironmentExtender
{
    private final OpenLdap openLdap;
    private final String imagesVersion;

    @Inject
    public OpenLdapReferral(OpenLdap openLdap, EnvironmentConfig environmentConfig)
    {
        this.openLdap = requireNonNull(openLdap, "openLdap is null");
        this.imagesVersion = environmentConfig.getImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String baseImage = format("ghcr.io/trinodb/testing/almalinux9-oj17-openldap-referrals:%s", imagesVersion);

        builder.configureContainers(dockerContainer -> {
            if (isTrinoContainer(dockerContainer.getLogicalName())) {
                dockerContainer.setDockerImageName(baseImage);
            }
        });

        builder.configureContainer(TESTS, dockerContainer -> dockerContainer.setDockerImageName(baseImage));
        builder.configureContainer(LDAP, dockerContainer -> dockerContainer.setDockerImageName(baseImage));
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(openLdap);
    }
}
