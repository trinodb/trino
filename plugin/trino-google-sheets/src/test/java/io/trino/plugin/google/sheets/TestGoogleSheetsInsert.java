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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
import com.google.api.services.sheets.v4.model.DeleteSheetRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static io.trino.plugin.google.sheets.SheetsClient.newBackOff;
import static io.trino.plugin.google.sheets.SheetsClient.newUnsuccessfulResponseHandler;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.DATA_SHEET_ID;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Math.toIntExact;

public class TestGoogleSheetsInsert
        extends AbstractTestQueryFramework
{
    private static final String APPLICATION_NAME = "trino google sheets integration test";
    private static final Logger log = Logger.get(TestGoogleSheetsInsert.class);

    private final Sheets sheetsService;
    private final String metadataSheetName;
    private final int metadataSheetId;

    public TestGoogleSheetsInsert()
            throws Exception
    {
        sheetsService = getSheetsService();
        metadataSheetName = "Tables" + randomNameSuffix();
        metadataSheetId = addSheet(TEST_METADATA_SHEET_ID, metadataSheetName);

        ValueRange headerRow = new ValueRange().setValues(ImmutableList.of(ImmutableList.of("Table Name", "Sheet ID")));
        insertRow(TEST_METADATA_SHEET_ID, metadataSheetName, headerRow);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SheetsQueryRunner.builder()
                .addConnectorProperty("gsheets.metadata-sheet-id", TEST_METADATA_SHEET_ID + "#" + metadataSheetName)
                .addConnectorProperty("gsheets.connection-timeout", "1m")
                .addConnectorProperty("gsheets.read-timeout", "1m")
                .build();
    }

    @AfterAll
    void afterAll()
    {
        dropSheet(TEST_METADATA_SHEET_ID, metadataSheetId);
    }

    @Test
    public void testInsertIntoTable()
            throws Exception
    {
        String tableName = "nation_insert_test_" + randomNameSuffix();

        int sheetId = addSheet(DATA_SHEET_ID, tableName);
        try {
            ValueRange headerRow = new ValueRange().setValues(ImmutableList.of(ImmutableList.of("nationkey", "name", "regionkey", "comment")));
            insertRow(DATA_SHEET_ID, tableName, headerRow);

            ValueRange metadataRow = new ValueRange().setValues(ImmutableList.of(ImmutableList.of(tableName, DATA_SHEET_ID + "#" + tableName)));
            insertRow(TEST_METADATA_SHEET_ID, metadataSheetName, metadataRow);

            assertEventually(
                    new Duration(1, TimeUnit.MINUTES),
                    new Duration(1, TimeUnit.SECONDS),
                    () -> assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0"));

            assertUpdate("INSERT INTO " + tableName + " SELECT cast(nationkey as varchar), cast(name as varchar), cast(regionkey as varchar), cast(comment as varchar) FROM tpch.tiny.nation", 25);
            assertEventually(
                    new Duration(5, TimeUnit.MINUTES),
                    new Duration(1, TimeUnit.SECONDS),
                    () -> assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation"));
        }
        finally {
            dropSheet(DATA_SHEET_ID, sheetId);
        }
    }

    private int addSheet(String spreadsheetId, String title)
            throws IOException
    {
        Request addSheetRequest = new Request()
                .setAddSheet(new AddSheetRequest()
                        .setProperties(new SheetProperties().setTitle(title)));
        BatchUpdateSpreadsheetRequest batchRequest = new BatchUpdateSpreadsheetRequest()
                .setRequests(ImmutableList.of(addSheetRequest));
        BatchUpdateSpreadsheetResponse batchResponse = sheetsService.spreadsheets()
                .batchUpdate(spreadsheetId, batchRequest)
                .execute();
        return batchResponse.getReplies().getFirst().getAddSheet().getProperties().getSheetId();
    }

    private void insertRow(String spreadsheetId, String sheetName, ValueRange content)
            throws IOException
    {
        sheetsService.spreadsheets().values()
                .append(spreadsheetId, sheetName, content)
                .setValueInputOption("RAW")
                .execute();
    }

    private void dropSheet(String spreadsheetId, int sheetId)
    {
        Request request = new Request().setDeleteSheet(new DeleteSheetRequest().setSheetId(sheetId));
        try {
            sheetsService.spreadsheets()
                    .batchUpdate(spreadsheetId, new BatchUpdateSpreadsheetRequest().setRequests(ImmutableList.of(request)))
                    .execute();
        }
        catch (IOException e) {
            log.warn("Failed to delete sheet with id %d in spreadsheet %s: %s", sheetId, spreadsheetId, e.getMessage());
        }
    }

    private Sheets getSheetsService()
            throws Exception
    {
        return new Sheets.Builder(
                newTrustedTransport(),
                JacksonFactory.getDefaultInstance(),
                setTimeout(getCredentials()))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    private GoogleCredential getCredentials()
            throws Exception
    {
        String credentialsKey = requiredNonEmptySystemProperty("testing.gsheets.credentials-key");
        return GoogleCredential.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(credentialsKey)))
                .createScoped(ImmutableList.of(SheetsScopes.SPREADSHEETS, SheetsScopes.DRIVE));
    }

    private static HttpRequestInitializer setTimeout(HttpRequestInitializer requestInitializer)
    {
        return httpRequest -> {
            requestInitializer.initialize(httpRequest);
            httpRequest.setConnectTimeout(toIntExact(TimeUnit.MINUTES.toMillis(1)));
            httpRequest.setReadTimeout(toIntExact(TimeUnit.MINUTES.toMillis(1)));
            httpRequest.setUnsuccessfulResponseHandler(newUnsuccessfulResponseHandler());
            httpRequest.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(newBackOff()));
        };
    }
}
