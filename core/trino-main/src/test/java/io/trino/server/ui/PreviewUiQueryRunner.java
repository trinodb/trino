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
package io.trino.server.ui;

import io.airlift.log.Logger;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.StandaloneQueryRunner;

import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class PreviewUiQueryRunner
{
    private PreviewUiQueryRunner() {}

    public static void main(String[] args)
    {
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("tiny")
                .build(), PreviewUiQueryRunner::configureTrinoServer);

        Logger log = Logger.get(PreviewUiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    private static void configureTrinoServer(TestingTrinoServer.Builder builder)
    {
        builder.addProperty("web-ui.preview.enabled", "true");
        builder.addProperty("http-server.http.port", "8080");
    }
}
