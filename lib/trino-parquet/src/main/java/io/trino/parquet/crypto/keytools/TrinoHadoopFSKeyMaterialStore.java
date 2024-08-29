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
package io.trino.parquet.crypto.keytools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.parquet.crypto.ParquetCryptoRuntimeException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class TrinoHadoopFSKeyMaterialStore
{
    public static final String KEY_MATERIAL_FILE_PREFIX = "_KEY_MATERIAL_FOR_";
    public static final String TEMP_FILE_PREFIX = "_TMP";
    public static final String KEY_MATERIAL_FILE_SUFFFIX = ".json";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private TrinoFileSystem trinoFileSystem;
    private Map<String, String> keyMaterialMap;
    private Location keyMaterialFile;

    TrinoHadoopFSKeyMaterialStore(TrinoFileSystem trinoFileSystem, Location parquetFilePath, boolean tempStore)
    {
        this.trinoFileSystem = trinoFileSystem;
        String fullPrefix = (tempStore ? TEMP_FILE_PREFIX : "");
        fullPrefix += KEY_MATERIAL_FILE_PREFIX;
        keyMaterialFile = parquetFilePath.parentDirectory().appendPath(
                fullPrefix + parquetFilePath.fileName() + KEY_MATERIAL_FILE_SUFFFIX);
    }

    public String getKeyMaterial(String keyIDInFile)
            throws ParquetCryptoRuntimeException
    {
        if (null == keyMaterialMap) {
            loadKeyMaterialMap();
        }
        return keyMaterialMap.get(keyIDInFile);
    }

    private void loadKeyMaterialMap()
    {
        TrinoInputFile inputfile = trinoFileSystem.newInputFile(keyMaterialFile);
        try (TrinoInputStream keyMaterialStream = inputfile.newStream()) {
            JsonNode keyMaterialJson = objectMapper.readTree(keyMaterialStream);
            keyMaterialMap = objectMapper.readValue(keyMaterialJson.traverse(),
                    new TypeReference<Map<String, String>>() {});
        }
        catch (FileNotFoundException e) {
            throw new ParquetCryptoRuntimeException("External key material not found at " + keyMaterialFile, e);
        }
        catch (IOException e) {
            throw new ParquetCryptoRuntimeException("Failed to get key material from " + keyMaterialFile, e);
        }
    }
}
