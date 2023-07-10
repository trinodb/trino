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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SheetsConfig
{
    private Optional<String> credentialsFilePath = Optional.empty();
    private Optional<String> credentialsKey = Optional.empty();
    private Optional<String> metadataSheetId = Optional.empty();
    private int sheetsDataMaxCacheSize = 1000;
    private Duration sheetsDataExpireAfterWrite = new Duration(5, TimeUnit.MINUTES);
    // 20s is the default timeout of com.google.api.client.http.HttpRequest
    private Duration connectionTimeout = new Duration(20, TimeUnit.SECONDS);
    private Duration readTimeout = new Duration(20, TimeUnit.SECONDS);
    private Duration writeTimeout = new Duration(20, TimeUnit.SECONDS);

    @AssertTrue(message = "Exactly one of 'gsheets.credentials-key' or 'gsheets.credentials-path' must be specified")
    public boolean isCredentialsConfigurationValid()
    {
        return credentialsKey.isPresent() ^ credentialsFilePath.isPresent();
    }

    @NotNull
    public Optional<@FileExists String> getCredentialsFilePath()
    {
        return credentialsFilePath;
    }

    @Config("gsheets.credentials-path")
    @LegacyConfig("credentials-path")
    @ConfigDescription("Credential file path to google service account")
    public SheetsConfig setCredentialsFilePath(String credentialsFilePath)
    {
        this.credentialsFilePath = Optional.ofNullable(credentialsFilePath);
        return this;
    }

    @NotNull
    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config("gsheets.credentials-key")
    @ConfigDescription("The base64 encoded credentials key")
    @ConfigSecuritySensitive
    public SheetsConfig setCredentialsKey(String credentialsKey)
    {
        this.credentialsKey = Optional.ofNullable(credentialsKey);
        return this;
    }

    @NotNull
    public Optional<String> getMetadataSheetId()
    {
        return metadataSheetId;
    }

    @Config("gsheets.metadata-sheet-id")
    @LegacyConfig("metadata-sheet-id")
    @ConfigDescription("Metadata sheet id containing table sheet mapping")
    public SheetsConfig setMetadataSheetId(String metadataSheetId)
    {
        this.metadataSheetId = Optional.ofNullable(metadataSheetId);
        return this;
    }

    @Min(1)
    public int getSheetsDataMaxCacheSize()
    {
        return sheetsDataMaxCacheSize;
    }

    @Config("gsheets.max-data-cache-size")
    @LegacyConfig("sheets-data-max-cache-size")
    @ConfigDescription("Sheet data max cache size")
    public SheetsConfig setSheetsDataMaxCacheSize(int sheetsDataMaxCacheSize)
    {
        this.sheetsDataMaxCacheSize = sheetsDataMaxCacheSize;
        return this;
    }

    @MinDuration("1m")
    public Duration getSheetsDataExpireAfterWrite()
    {
        return sheetsDataExpireAfterWrite;
    }

    @Config("gsheets.data-cache-ttl")
    @LegacyConfig("sheets-data-expire-after-write")
    @ConfigDescription("Sheets data expire after write duration")
    public SheetsConfig setSheetsDataExpireAfterWrite(Duration sheetsDataExpireAfterWriteMinutes)
    {
        this.sheetsDataExpireAfterWrite = sheetsDataExpireAfterWriteMinutes;
        return this;
    }

    @MinDuration("0ms")
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("gsheets.connection-timeout")
    @ConfigDescription("Timeout when connection to Google Sheets API")
    public SheetsConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @MinDuration("0ms")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("gsheets.read-timeout")
    @ConfigDescription("Timeout when reading from Google Sheets API")
    public SheetsConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @MinDuration("0ms")
    public Duration getWriteTimeout()
    {
        return writeTimeout;
    }

    @Config("gsheets.write-timeout")
    @ConfigDescription("Timeout when writing to Google Sheets API")
    public SheetsConfig setWriteTimeout(Duration writeTimeout)
    {
        this.writeTimeout = writeTimeout;
        return this;
    }
}
