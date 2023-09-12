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
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
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
import java.util.Set;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_INSERT_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsClient
{
    public static final String DEFAULT_RANGE = "$1:$10000";
    public static final String RANGE_SEPARATOR = "#";
    private static final Logger log = Logger.get(SheetsClient.class);

    private static final String APPLICATION_NAME = "trino google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS);

    private static final String INSERT_VALUE_OPTION = "RAW";
    private static final String INSERT_DATA_OPTION = "INSERT_ROWS";

    private final NonEvictableLoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;

    private final Optional<String> metadataSheetId;

    private final Sheets sheetsService;

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.metadataSheetId = config.getMetadataSheetId();

        try {
            this.sheetsService = new Sheets.Builder(newTrustedTransport(), JSON_FACTORY, setTimeout(getCredentials(config), config)).setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).maximumSize(maxCacheSize),
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
            return getTable(namedTableHandle.getTableName());
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
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<String> header = stringValues.get(0);
            int count = 0;
            for (String column : header) {
                String columnValue = column.toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                columns.add(new SheetsColumn(columnValue, VarcharType.VARCHAR));
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
        SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange(sheetExpression);
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

    private static Credential getCredentials(SheetsConfig sheetsConfig)
    {
        if (sheetsConfig.getCredentialsFilePath().isPresent()) {
            try (InputStream in = new FileInputStream(sheetsConfig.getCredentialsFilePath().get())) {
                return credentialFromStream(in);
            }
            catch (IOException e) {
                throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
            }
        }

        if (sheetsConfig.getCredentialsKey().isPresent()) {
            try {
                return credentialFromStream(
                                new ByteArrayInputStream(Base64.getDecoder().decode(sheetsConfig.getCredentialsKey().get())));
            }
            catch (IOException e) {
                throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
            }
        }

        throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, "No sheets credentials were provided");
    }

    private static Credential credentialFromStream(InputStream inputStream)
            throws IOException
    {
        return GoogleCredential.fromStream(inputStream).createScoped(SCOPES);
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression)
    {
        try {
            // by default loading up to 10k rows from the first tab of the sheet
            String defaultRange = DEFAULT_RANGE;
            String[] tableOptions = sheetExpression.split(RANGE_SEPARATOR);
            String sheetId = tableOptions[0];
            if (tableOptions.length > 1) {
                defaultRange = tableOptions[1];
            }
            log.debug("Accessing sheet id [%s] with range [%s]", sheetId, defaultRange);
            List<List<Object>> values = sheetsService.spreadsheets().values().get(sheetId, defaultRange).execute().getValues();
            if (values == null) {
                throw new TrinoException(SHEETS_TABLE_LOAD_ERROR, "No non-empty cells found in sheet: " + sheetExpression);
            }
            return values;
        }
        catch (IOException e) {
            // TODO: improve error to a {Table|Sheet}NotFoundException
            // is a backwards incompatible error code change from SHEETS_UNKNOWN_TABLE_ERROR -> NOT_FOUND
            throw new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Failed reading data from sheet: " + sheetExpression, e);
        }
    }

    private HttpRequestInitializer setTimeout(HttpRequestInitializer requestInitializer, SheetsConfig config)
    {
        requireNonNull(config.getConnectionTimeout(), "connectionTimeout is null");
        requireNonNull(config.getReadTimeout(), "readTimeout is null");
        requireNonNull(config.getWriteTimeout(), "writeTimeout is null");

        return httpRequest -> {
            requestInitializer.initialize(httpRequest);
            httpRequest.setConnectTimeout(toIntExact(config.getConnectionTimeout().toMillis()));
            httpRequest.setReadTimeout(toIntExact(config.getReadTimeout().toMillis()));
            httpRequest.setWriteTimeout(toIntExact(config.getWriteTimeout().toMillis()));
        };
    }
}
