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
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentDefaults;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeLdap;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeLdapAndFile;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeLdapBindDn;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeLdapInsecure;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeLdapReferrals;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.TestGroups.LDAP;
import static io.trino.tests.product.TestGroups.LDAP_AND_FILE;
import static io.trino.tests.product.TestGroups.LDAP_AND_FILE_CLI;
import static io.trino.tests.product.TestGroups.LDAP_CLI;
import static io.trino.tests.product.TestGroups.LDAP_MULTIPLE_BINDS;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteLdap
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                testOnEnvironment(EnvSinglenodeLdap.class).withGroups(LDAP).build(),
                testOnEnvironment(EnvSinglenodeLdapAndFile.class).withGroups(LDAP, LDAP_AND_FILE, LDAP_CLI, LDAP_AND_FILE_CLI).build(),
                testOnEnvironment(EnvSinglenodeLdapInsecure.class).withGroups(LDAP).build(),
                testOnEnvironment(EnvSinglenodeLdapReferrals.class).withGroups(LDAP).build(),
                testOnEnvironment(EnvSinglenodeLdapBindDn.class).withGroups(LDAP).withExcludedGroups(LDAP_MULTIPLE_BINDS).build());
    }
}
