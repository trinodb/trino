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
package io.trino.tests.product.gcs;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

public final class GcsTableFormatsTestUtils
{
    private GcsTableFormatsTestUtils() {}

    public static void verifyLocationContainsDiscouragedCharacter(GcsEnvironment environment, String catalog)
    {
        // According to https://docs.cloud.google.com/storage/docs/objects#recommendations some chars ([*]#?) are discouraged
        // because they are specially treated in gcloud cli. But they are not directly prohibited.
        // Chars used in schema location are not escaped like those in partition names
        // so it allows to test whether whole stack works e2e with discouraged characters.
        String schemaName = catalog + ".test";
        String tableName = schemaName + ".test_location_contains_discouraged_character_" + randomNameSuffix();
        String locationWithDiscouragedChars = environment.getWarehouseDirectory() + "/[*]#?";

        try {
            environment.executeTrinoUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + locationWithDiscouragedChars + "')");
            environment.executeTrinoUpdate("CREATE TABLE " + tableName + " (id bigint, someValue varchar)");
            environment.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (1, 'someValue')");

            assertThat(environment.executeTrino("SELECT * FROM " + tableName))
                    .containsOnly(row(1, "someValue"));
        }
        finally {
            environment.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
            environment.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }
}
