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

public abstract class SuiteDeltaLakeDatabricks
        extends Suite
{
    protected String[] getExcludedTests()
    {
        return new String[] {
                // AWS Glue does not support table comments
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testDeltaToHiveCommentTable",
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testHiveToDeltaCommentTable",
                "io.trino.tests.product.deltalake.TestDeltaLakeAlterTableCompatibility.testCommentOnTableUnsupportedWriterVersion",
                // AWS Glue does not support column comments
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testDeltaToHiveCommentColumn",
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testHiveToDeltaCommentColumn",
                "io.trino.tests.product.deltalake.TestDeltaLakeAlterTableCompatibility.testCommentOnColumnUnsupportedWriterVersion",
                // AWS Glue does not support table renames
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testDeltaToHiveAlterTable",
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testHiveToDeltaAlterTable",
        };
    }
}
