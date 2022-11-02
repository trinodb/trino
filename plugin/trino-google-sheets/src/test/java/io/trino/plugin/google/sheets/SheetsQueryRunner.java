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
package io.trino.plugin.google.sheets;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.getTestCredentialsPath;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class SheetsQueryRunner
{
    protected static final String GOOGLE_SHEETS = "gsheets";

    private SheetsQueryRunner() {}

    public static DistributedQueryRunner createSheetsQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("credentials-path", getTestCredentialsPath());
            connectorProperties.putIfAbsent("metadata-sheet-id", TEST_METADATA_SHEET_ID);
            connectorProperties.putIfAbsent("sheets-data-max-cache-size", "1000");
            connectorProperties.putIfAbsent("sheets-data-expire-after-write", "5m");

            queryRunner.installPlugin(new SheetsPlugin());
            queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, connectorProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GOOGLE_SHEETS)
                .setSchema("default")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createSheetsQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of());

        Logger log = Logger.get(SheetsQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
