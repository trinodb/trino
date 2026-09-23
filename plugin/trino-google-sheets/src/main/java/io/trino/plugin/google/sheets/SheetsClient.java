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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import io.trino.spi.type.VarcharType;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_EXCEEDED_ROW_LIMIT;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_INSERT_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_INVALID_TABLE_FORMAT;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsClient
{
    public static final String RANGE_SEPARATOR = "#";
    private static final Logger log = Logger.get(SheetsClient.class);

    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    // A range without a sheet name refers to the first visible sheet, and column-only bounds cover every row of it.
    // Google Sheets supports at most 18,278 columns, so ZZZ is the last column a sheet can have.
    static final String ENTIRE_FIRST_SHEET_RANGE = "A:ZZZ";
    // Search range for spreadsheets.values.append. It does not bound the write: the API picks the last table
    // inside the range and appends after it, starting at that table's first column. Its exact value therefore
    // decides which block an insert lands in on a sheet that holds more than one, so it is kept as it has
    // always been. Both narrowing it to A1 and widening it to the whole sheet retarget existing inserts,
    // so this is deliberately not derived from 'gsheets.max-rows'.
    static final String APPEND_TABLE_SEARCH_RANGE = "$1:$10000";

    private static final String APPLICATION_NAME = "trino google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS);

    private static final String INSERT_VALUE_OPTION = "RAW";
    private static final String INSERT_DATA_OPTION = "INSERT_ROWS";

    private final NonEvictableLoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;

    private final Optional<String> metadataSheetId;
    // Empty when the row limit is disabled
    private final OptionalInt maxRows;

    private final Sheets sheetsService;

    @Inject
    public SheetsClient(SheetsConfig config)
    {
        this(config, createSheetsService(config));
    }

    @VisibleForTesting
    SheetsClient(SheetsConfig config, Sheets sheetsService)
    {
        this.metadataSheetId = config.getMetadataSheetId();
        int maxRows = config.getMaxRows();
        this.maxRows = maxRows == 0 ? OptionalInt.empty() : OptionalInt.of(maxRows);
        this.sheetsService = requireNonNull(sheetsService, "sheetsService is null");

        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(ofMillis(expiresAfterWriteMillis)).maximumSize(maxCacheSize),
                new CacheLoader<>()
                {
                    @Override
                    public Optional<String> load(String tableName)
                    {
                        return getSheetExpressionForTable(tableName);
                    }

                    @Override
                    public Map<String, Optional<String>> loadAll(Iterable<? extends String> tableList)
                    {
                        return getAllTableSheetExpressionMapping();
                    }
                });

        this.sheetDataCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .maximumSize(maxCacheSize)
                .build(CacheLoader.from(this::readAllValuesFromSheetExpression));
    }

    public Optional<SheetsTable> getTable(SheetsConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof SheetsNamedTableHandle namedTableHandle) {
            return getTable(namedTableHandle.tableName());
        }
        if (tableHandle instanceof SheetsSheetTableHandle sheetTableHandle) {
            return getTableFromValues(readAllValuesFromSheet(sheetTableHandle.getSheetExpression()));
        }
        throw new IllegalStateException("Found unexpected table handle type " + tableHandle);
    }

    public Optional<SheetsTable> getTable(String tableName)
    {
        List<List<Object>> values = readAllValues(tableName);
        return getTableFromValues(values);
    }

    public Optional<SheetsTable> getTableFromValues(List<List<Object>> values)
    {
        List<List<String>> stringValues = convertToStringValues(values);
        if (stringValues.size() > 0) {
            ImmutableList.Builder<SheetsColumnHandle> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<String> header = stringValues.get(0);
            int count = 0;
            for (int i = 0; i < header.size(); i++) {
                String columnValue = header.get(i).toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                columns.add(new SheetsColumnHandle(columnValue, VarcharType.VARCHAR, i));
            }
            List<List<String>> dataValues = stringValues.subList(1, values.size()); // removing header info
            return Optional.of(new SheetsTable(columns.build(), dataValues));
        }
        return Optional.empty();
    }

    public Set<String> getTableNames()
    {
        if (metadataSheetId.isEmpty()) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            List<List<Object>> tableMetadata = sheetDataCache.getUnchecked(metadataSheetId.get());
            for (int i = 1; i < tableMetadata.size(); i++) {
                if (tableMetadata.get(i).size() > 0) {
                    tables.add(String.valueOf(tableMetadata.get(i).get(0)));
                }
            }
            return tables.build();
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_METASTORE_ERROR, e);
        }
    }

    public List<List<Object>> readAllValues(String tableName)
    {
        try {
            String sheetExpression = getCachedSheetExpressionForTable(tableName);
            return readAllValuesFromSheet(sheetExpression);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for table: " + tableName, e);
        }
    }

    public List<List<Object>> readAllValuesFromSheet(String sheetExpression)
    {
        try {
            return sheetDataCache.getUnchecked(sheetExpression);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for sheet: " + sheetExpression, e);
        }
    }

    public void insertIntoSheet(String sheetExpression, List<List<Object>> rows)
    {
        ValueRange body = new ValueRange().setValues(rows);
        // 'gsheets.max-rows' is a read limit and has no meaning for appends, see APPEND_TABLE_SEARCH_RANGE
        SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange(sheetExpression, APPEND_TABLE_SEARCH_RANGE);
        try {
            sheetsService.spreadsheets().values().append(sheetIdAndRange.getSheetId(), sheetIdAndRange.getRange(), body)
                    .setValueInputOption(INSERT_VALUE_OPTION)
                    .setInsertDataOption(INSERT_DATA_OPTION)
                    .execute();
        }
        catch (IOException e) {
            throw new TrinoException(SHEETS_INSERT_ERROR, "Error inserting data to sheet: ", e);
        }

        // Flush the cache contents for the table that was written to.
        // This is a best-effort solution, since the Google Sheets API seems to be eventually consistent.
        // If the table written to will be queried directly afterward the inserts might not have been propagated yet.
        // and the users needs to wait till the cached version alters out.
        sheetDataCache.invalidate(sheetExpression);
    }

    public static List<List<String>> convertToStringValues(List<List<Object>> values)
    {
        return values.stream()
                .map(columns -> columns.stream().map(String::valueOf).collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private Optional<String> getSheetExpressionForTable(String tableName)
    {
        Map<String, Optional<String>> tableSheetMap = getAllTableSheetExpressionMapping();
        if (!tableSheetMap.containsKey(tableName)) {
            return Optional.empty();
        }
        return tableSheetMap.get(tableName);
    }

    public String getCachedSheetExpressionForTable(String tableName)
    {
        return tableSheetMappingCache.getUnchecked(tableName)
                .orElseThrow(() -> new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName));
    }

    private Map<String, Optional<String>> getAllTableSheetExpressionMapping()
    {
        if (metadataSheetId.isEmpty()) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, Optional<String>> tableSheetMap = ImmutableMap.builder();
        List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId.get());
        // first line is assumed to be sheet header
        for (int i = 1; i < data.size(); i++) {
            if (data.get(i).size() >= 2) {
                String tableId = String.valueOf(data.get(i).get(0));
                String sheetId = String.valueOf(data.get(i).get(1));
                tableSheetMap.put(tableId.toLowerCase(Locale.ENGLISH), Optional.of(sheetId));
            }
        }
        return tableSheetMap.buildOrThrow();
    }

    private static Sheets createSheetsService(SheetsConfig config)
    {
        try {
            return new Sheets.Builder(newTrustedTransport(), JSON_FACTORY, setTimeout(getCredentials(config), config))
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    private static Credential getCredentials(SheetsConfig sheetsConfig)
    {
        if (sheetsConfig.getCredentialsFilePath().isPresent()) {
            try (InputStream in = new FileInputStream(sheetsConfig.getCredentialsFilePath().get())) {
                return credentialFromStream(in, sheetsConfig.getDelegatedUserEmail());
            }
            catch (IOException e) {
                throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
            }
        }

        if (sheetsConfig.getCredentialsKey().isPresent()) {
            try {
                return credentialFromStream(
                        new ByteArrayInputStream(Base64.getDecoder().decode(sheetsConfig.getCredentialsKey().get())), sheetsConfig.getDelegatedUserEmail());
            }
            catch (IOException e) {
                throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
            }
        }

        throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, "No sheets credentials were provided");
    }

    private static Credential credentialFromStream(InputStream inputStream, Optional<String> delegatedUserEmail)
            throws IOException
    {
        GoogleCredential credential = GoogleCredential.fromStream(inputStream).createScoped(SCOPES);
        return delegatedUserEmail.map(credential::createDelegated).orElse(credential);
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression)
    {
        try {
            SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange(sheetExpression, defaultRange(maxRows));
            String sheetId = sheetIdAndRange.getSheetId();
            String range = sheetIdAndRange.getRange();
            log.debug("Accessing sheet id [%s] with range [%s]", sheetId, range);
            List<List<Object>> values = sheetsService.spreadsheets().values().get(sheetId, range).execute().getValues();
            if (values == null) {
                throw new TrinoException(SHEETS_INVALID_TABLE_FORMAT, "No non-empty cells found in sheet: " + sheetExpression);
            }
            if (maxRows.isPresent() && values.size() > maxRows.orElseThrow()) {
                throw new TrinoException(
                        SHEETS_EXCEEDED_ROW_LIMIT,
                        "Sheet %s has more than %s rows. Specify a range with fewer rows or increase 'gsheets.max-rows'".formatted(sheetExpression, maxRows.orElseThrow()));
            }
            return values;
        }
        catch (IOException e) {
            // TODO: improve error to a {Table|Sheet}NotFoundException
            // is a backwards incompatible error code change from SHEETS_UNKNOWN_TABLE_ERROR -> NOT_FOUND
            throw new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Failed reading data from sheet: " + sheetExpression, e);
        }
    }

    /**
     * Range read when a sheet expression does not specify one. A range without a sheet name refers to the
     * first visible sheet. One row more than the limit is requested, so that a sheet exceeding the limit is
     * detected instead of being silently truncated.
     */
    @VisibleForTesting
    static String defaultRange(OptionalInt maxRows)
    {
        if (maxRows.isEmpty()) {
            return ENTIRE_FIRST_SHEET_RANGE;
        }
        return "$1:$" + (maxRows.orElseThrow() + 1L);
    }

    private static HttpRequestInitializer setTimeout(HttpRequestInitializer requestInitializer, SheetsConfig config)
    {
        requireNonNull(config.getConnectionTimeout(), "connectionTimeout is null");
        requireNonNull(config.getReadTimeout(), "readTimeout is null");
        requireNonNull(config.getWriteTimeout(), "writeTimeout is null");

        return httpRequest -> {
            requestInitializer.initialize(httpRequest);
            httpRequest.setConnectTimeout(toIntExact(config.getConnectionTimeout().toMillis()));
            httpRequest.setReadTimeout(toIntExact(config.getReadTimeout().toMillis()));
            httpRequest.setWriteTimeout(toIntExact(config.getWriteTimeout().toMillis()));
            httpRequest.setUnsuccessfulResponseHandler(newUnsuccessfulResponseHandler());
            httpRequest.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(newBackOff()));
        };
    }

    @VisibleForTesting
    static HttpBackOffUnsuccessfulResponseHandler newUnsuccessfulResponseHandler()
    {
        return new HttpBackOffUnsuccessfulResponseHandler(newBackOff())
                .setBackOffRequired(response -> response.getStatusCode() == HTTP_TOO_MANY_REQUESTS ||
                        (response.getStatusCode() >= 500 && response.getStatusCode() <= 599));
    }

    @VisibleForTesting
    static ExponentialBackOff newBackOff()
    {
        return new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(500)
                .setMaxIntervalMillis(10_000)
                .setMaxElapsedTimeMillis(60_000)
                .setMultiplier(1.5)
                .build();
    }
}
