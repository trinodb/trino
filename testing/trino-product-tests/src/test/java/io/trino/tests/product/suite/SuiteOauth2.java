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
package io.trino.tests.product.suite;

import io.trino.tests.product.TestGroup;
import io.trino.tests.product.jdbc.JdbcOAuth2AuthenticatedHttpProxyEnvironment;
import io.trino.tests.product.jdbc.JdbcOAuth2AuthenticatedHttpsProxyEnvironment;
import io.trino.tests.product.jdbc.JdbcOAuth2BasicEnvironment;
import io.trino.tests.product.jdbc.JdbcOAuth2HttpProxyEnvironment;
import io.trino.tests.product.jdbc.JdbcOAuth2HttpsProxyEnvironment;
import io.trino.tests.product.jdbc.JdbcOAuth2RefreshEnvironment;
import io.trino.tests.product.jdbc.JdbcOidcEnvironment;
import io.trino.tests.product.jdbc.JdbcOidcRefreshEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

public final class SuiteOauth2
{
    private SuiteOauth2() {}

    public static void main(String[] args)
    {
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        results.add(SuiteRunner.forEnvironment(JdbcOAuth2BasicEnvironment.class)
                .includeTag(TestGroup.Oauth2.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOidcEnvironment.class)
                .includeTag(TestGroup.Oauth2.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOAuth2HttpProxyEnvironment.class)
                .includeTag(TestGroup.Oauth2.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOAuth2HttpsProxyEnvironment.class)
                .includeTag(TestGroup.Oauth2.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOAuth2AuthenticatedHttpProxyEnvironment.class)
                .includeTag(TestGroup.Oauth2.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOAuth2AuthenticatedHttpsProxyEnvironment.class)
                .includeTag(TestGroup.Oauth2.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOAuth2RefreshEnvironment.class)
                .includeTag(TestGroup.Oauth2Refresh.class)
                .run());

        results.add(SuiteRunner.forEnvironment(JdbcOidcRefreshEnvironment.class)
                .includeTag(TestGroup.Oauth2Refresh.class)
                .run());

        SuiteRunner.printSummary(results);
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
