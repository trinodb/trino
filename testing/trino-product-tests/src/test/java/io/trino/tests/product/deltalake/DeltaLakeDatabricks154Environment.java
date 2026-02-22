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
package io.trino.tests.product.deltalake;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

public class DeltaLakeDatabricks154Environment
        extends DeltaLakeDatabricksEnvironment
{
    @Override
    protected String databricksJdbcUrl()
    {
        String rawUrl = appendJdbcOption(requireEnv("DATABRICKS_154_JDBC_URL"), "EnableArrow=0");
        return appendJdbcOption(rawUrl, "SocketTimeout=120");
    }

    static void main(String[] args)
    {
        try (DeltaLakeDatabricks154Environment environment = new DeltaLakeDatabricks154Environment()) {
            environment.start();
            System.out.println("DeltaLakeDatabricks154Environment started. Press Ctrl+C to stop.");
            Thread.sleep(Long.MAX_VALUE);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
