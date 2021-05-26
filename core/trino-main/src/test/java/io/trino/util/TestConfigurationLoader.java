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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.util.ConfigurationLoader.loadPropertiesFrom;
import static java.nio.file.Files.createTempFile;
import static org.testng.Assert.assertEquals;

public class TestConfigurationLoader
{
    @Test
    public void testConfigFileContainingSpecialCharacters()
            throws Exception
    {
        Path config = createTempFile("ldapConfig", "");

        String propKey = "ldap.user-bind-pattern";
        String propValue = "CN=${USER},OU=user,DC=example,DC=com:CN=${USER},OU=사용자,DC=example,DC=com:CN=${USER},OU=使用者,DC=example,DC=com:CN=${USER},OU=用戶,DC=example,DC=com:CN=${USER},OU=المستعمل,DC=example,DC=com:CN=${USER},OU=ユーザー,DC=example,DC=com";

        Files.write(config, ImmutableList.of(propKey + "=" + propValue));

        Map<String, String> property = loadPropertiesFrom(config.toAbsolutePath().toString());

        assertEquals(property.get(propKey), propValue);
    }
}
