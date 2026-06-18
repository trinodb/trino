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
package io.trino.encrypt;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.SimpleStringPBEConfig;
import org.jasypt.iv.StringFixedIvGenerator;
import org.jasypt.salt.StringFixedSaltGenerator;

public class TrinoEncryptor
{
    private static final long generatorUID = 7349610062158259284L;
    private StandardPBEStringEncryptor encryptor;

    public TrinoEncryptor()
    {
        this.encryptor = new StandardPBEStringEncryptor();
        SimpleStringPBEConfig config = new SimpleStringPBEConfig();
        // default PBEWithMD5AndDES, PBEWithMD5AndTripleDES PBEWITHHMACSHA512ANDAES_256(command line is not available)
        config.setAlgorithm("PBEWITHHMACSHA512ANDAES_256");
        config.setKeyObtentionIterations("1000");
        config.setProviderName("SunJCE");
        config.setSaltGenerator(new StringFixedSaltGenerator(String.valueOf(generatorUID)));
        config.setIvGenerator(new StringFixedIvGenerator(String.valueOf(generatorUID)));
        config.setStringOutputType("base64");
        encryptor.setConfig(config);
    }

    public String encrypt(String key, String value)
    {
        // encrypt-key
        encryptor.setPassword(key);
        return encryptor.encrypt(value);
    }
}
