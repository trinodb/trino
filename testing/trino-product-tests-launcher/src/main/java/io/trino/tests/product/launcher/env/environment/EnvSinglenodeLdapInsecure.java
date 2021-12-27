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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

@TestsEnvironment
public class EnvSinglenodeLdapInsecure
        extends AbstractEnvSinglenodeLdap
{
    private final PortBinder portBinder;

    @Inject
    public EnvSinglenodeLdapInsecure(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, PortBinder portBinder, EnvironmentConfig config)
    {
        super(ImmutableList.of(standard, hadoop), dockerFiles, portBinder, config);
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        super.extendEnvironment(builder);
        builder.configureContainer("ldapserver", container -> portBinder.exposePort(container, 389));
    }

    @Override
    protected String getPasswordAuthenticatorConfigPath()
    {
        return "conf/environment/singlenode-ldap-without-ssl/password-authenticator.properties";
    }
}
