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
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

@TestsEnvironment
public class EnvSinglenodeLdapBindDn
        extends AbstractEnvSinglenodeLdap
{
    @Inject
    public EnvSinglenodeLdapBindDn(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, PortBinder portBinder, EnvironmentConfig environmentConfig)
    {
        super(ImmutableList.of(standard, hadoop), dockerFiles, portBinder, environmentConfig);
    }

    @Override
    protected String getPasswordAuthenticatorConfigPath()
    {
        return "conf/environment/singlenode-ldap-bind-dn/password-authenticator.properties";
    }
}
