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
package io.prestosql.plugin.google.sheets;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.Permission;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.List;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_GOOGLE_DRIVE_BAD_CREDENTIALS_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_GOOGLE_DRIVE_PERMISSION_ERROR;
import static java.util.Objects.requireNonNull;

public class GoogleDriveClient
{
    private static final String APPLICATION_NAME = "presto google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final List<String> SCOPES = ImmutableList.of(DriveScopes.DRIVE_FILE);
    private static final String FIELD = "id";
    private final String credentialsFilePath;
    private final Drive driveService;
    private final String drivePermissionType;
    private final String drivePermissionRole;
    private final String drivePermissionEmailAddress;

    @Inject
    public GoogleDriveClient(SheetsConfig config)
    {
        requireNonNull(config, "config is null");
        this.credentialsFilePath = config.getCredentialsFilePath();
        this.drivePermissionType = config.getDrivePermissionType();
        this.drivePermissionRole = config.getDrivePermissionRole();
        this.drivePermissionEmailAddress = config.getDrivePermissionEmailAddress();
        try {
            this.driveService = new Drive.Builder(newTrustedTransport(), JSON_FACTORY,
                    getCredentials()).setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(SHEETS_GOOGLE_DRIVE_BAD_CREDENTIALS_ERROR, e);
        }
    }

    private Credential getCredentials()
    {
        try (InputStream in = new FileInputStream(credentialsFilePath)) {
            return GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    public void grantPermission(String sheetId)
    {
        Permission userPermission = new Permission()
                .setType(drivePermissionType)
                .setRole(drivePermissionRole)
                .setEmailAddress(drivePermissionEmailAddress);
        try {
            driveService.permissions().create(sheetId, userPermission).setFields(FIELD).execute();
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_GOOGLE_DRIVE_PERMISSION_ERROR, e);
        }
    }
}
