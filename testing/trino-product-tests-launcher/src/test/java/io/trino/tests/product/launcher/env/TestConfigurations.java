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
package io.trino.tests.product.launcher.env;

import io.trino.tests.product.launcher.env.environment.EnvMultinodeSqlserver;
import io.trino.tests.product.launcher.env.jdk.TemurinJdkProvider;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.tests.product.launcher.Configurations.canonicalEnvironmentName;
import static io.trino.tests.product.launcher.Configurations.canonicalJdkProviderName;
import static io.trino.tests.product.launcher.Configurations.nameForJdkProviderName;
import static io.trino.tests.product.launcher.Configurations.nameForSuiteClass;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConfigurations
{
    @Test
    public void testCanonicalEnvironmentName()
    {
        // canonical environment name should be retain as is
        assertThat(canonicalEnvironmentName("ala")).isEqualTo("ala");
        assertThat(canonicalEnvironmentName("duza-ala")).isEqualTo("duza-ala");
        assertThat(canonicalEnvironmentName("duza-Ala")).isEqualTo("duza-ala");

        // a name of the class (as if copy-pasted from IDE) should result in canonical environment name
        assertThat(canonicalEnvironmentName("Ala")).isEqualTo("ala");
        assertThat(canonicalEnvironmentName("DuzaAla")).isEqualTo("duza-ala");
        assertThat(canonicalEnvironmentName("EnvDuzaAla")).isEqualTo("duza-ala");
        // real life example
        assertThat(canonicalEnvironmentName(EnvMultinodeSqlserver.class.getSimpleName())).isEqualTo("multinode-sqlserver");

        // document current state; this behavior is neither intentional or (currently) forbidden
        assertThat(canonicalEnvironmentName("duza----Ala")).isEqualTo("duza-ala");
    }

    @Test
    public void testSuiteName()
    {
        // suite name with a word
        assertThat(nameForSuiteClass(SuiteSnowflakeStyle.class)).isEqualTo("suite-snowflake-style");
        // suite name with word and number suffix
        assertThat(nameForSuiteClass(SuiteDeltaLakeDatabricks133Style.class)).isEqualTo("suite-delta-lake-databricks133-style");
    }

    @Test
    public void testJdkProviderName()
    {
        assertThat(nameForJdkProviderName(TemurinJdkProvider.class)).isEqualTo("temurin");
        assertThat(canonicalJdkProviderName("BuiltIN")).isEqualTo("builtin");
        assertThat(canonicalJdkProviderName("built-IN")).isEqualTo("builtin");
    }

    private static class SuiteSnowflakeStyle
            extends Suite
    {
        @Override
        public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
        {
            return List.of();
        }
    }

    private static class SuiteDeltaLakeDatabricks133Style
            extends Suite
    {
        @Override
        public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
        {
            return List.of();
        }
    }
}
