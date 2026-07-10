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
package io.trino.util;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.SimpleStringPBEConfig;
import org.jasypt.iv.StringFixedIvGenerator;
import org.jasypt.salt.StringFixedSaltGenerator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class CatalogStoreUtil
{
    private static final Logger log = Logger.get(CatalogStoreUtil.class);
    private static final long generatorUID = 7349610062158259284L;
    public static final String ENCRYPTED_PROPERTIES = "encrypted-properties";
    public static final String ENCRYPTED_KEY = "encrypted-key";

    private StandardPBEStringEncryptor standardPbeStringEncryptor;

    public CatalogStoreUtil()
    {
        this.standardPbeStringEncryptor = new StandardPBEStringEncryptor();
    }

    /**
     * Decrypt the encrypted properties, and
     * "encrypted.properties"
     *
     * @param properties the properties of catalog.
     */
    public void decryptEncryptedProperties(Map<String, String> properties)
    {
        setupStringEncryptor(properties);
        String encryptedPropertiesValue = properties.remove(ENCRYPTED_PROPERTIES);
        Set<String> encryptedProperties = splitEncryptedProperties(encryptedPropertiesValue);
        encryptedProperties.forEach(propertyName -> {
            String cipherText = properties.get(propertyName);
            if (cipherText != null) {
                String plainText = standardPbeStringEncryptor.decrypt(cipherText);
                properties.put(propertyName, plainText);
            }
        });
    }

    private Set<String> splitEncryptedProperties(String encryptedPropertyNamesValue)
    {
        if (encryptedPropertyNamesValue == null || encryptedPropertyNamesValue.isEmpty()) {
            return ImmutableSet.of();
        }
        String[] encryptedPropertyNameArray = encryptedPropertyNamesValue.split(",");
        Set<String> encryptedPropertyNames = new HashSet<>();
        for (String encryptedPropertyName : encryptedPropertyNameArray) {
            encryptedPropertyNames.add(encryptedPropertyName.trim());
        }
        return encryptedPropertyNames;
    }

    private void setupStringEncryptor(Map<String, String> properties)
    {
        if (!standardPbeStringEncryptor.isInitialized()) {
            SimpleStringPBEConfig config = new SimpleStringPBEConfig();
            // default PBEWithMD5AndDES, PBEWithMD5AndTripleDES PBEWITHHMACSHA512ANDAES_256(command line is not available)
            config.setAlgorithm("PBEWITHHMACSHA512ANDAES_256");
            config.setKeyObtentionIterations("1000");
            config.setProviderName("SunJCE");
            config.setSaltGenerator(new StringFixedSaltGenerator(String.valueOf(generatorUID)));
            config.setIvGenerator(new StringFixedIvGenerator(String.valueOf(generatorUID)));
            config.setStringOutputType("base64");
            standardPbeStringEncryptor.setConfig(config);

            String encryptedPassword = properties.remove(ENCRYPTED_KEY);
            if (encryptedPassword != null) {
                standardPbeStringEncryptor.setPassword(encryptedPassword);
            }
        }
    }
}
