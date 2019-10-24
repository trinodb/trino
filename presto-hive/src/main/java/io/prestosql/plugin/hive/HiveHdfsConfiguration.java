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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.hive.util.ConfigurationUtils.copy;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.getInitialConfiguration;
import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            initializer.initializeConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationInitializer initializer;
    private final Set<DynamicConfigurationProvider> dynamicProviders;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationInitializer initializer, Set<DynamicConfigurationProvider> dynamicProviders)
    {
        this.initializer = requireNonNull(initializer, "initializer is null");
        this.dynamicProviders = ImmutableSet.copyOf(requireNonNull(dynamicProviders, "dynamicProviders is null"));
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        if (dynamicProviders.isEmpty()) {
            // use the same configuration for everything
            return hadoopConfiguration.get();
        }
        Configuration config = copy(hadoopConfiguration.get());
        for (DynamicConfigurationProvider provider : dynamicProviders) {
            provider.updateConfiguration(config, context, uri);
        }

        //there's a presto bug that workers get toString here instead of the actual getName value
        if (context.getIdentity().getPrincipal().isPresent()) {
            String token = parseToken(context.getIdentity().getPrincipal());
            config.set("v3io.client.session.access-key", token);
        }

        return config;
    }

    // The following code has been added to support V3IO authentication
    private String parseToken(Optional<Principal> principalOption)
    {
        if (principalOption.isPresent()) {
            Principal principal = principalOption.get();
            String name = principal.getName();
            if (name.contains("V3IOPrincipal")) {
                String splitter = "uid:=";
                String[] tokenparse = name.split(splitter);
                if (tokenparse.length != 2) {
                    throw new RuntimeException(String.format("could not parse token from v3io principal, name='%s', delimiter='%s'", name, splitter));
                }
                else {
                    String encryptedToken = tokenparse[1].substring(0, tokenparse[1].length() - 1);
                    String token;
                    try {
                        token = new TokenHandler().decrypt(
                                Base64.getDecoder().decode(encryptedToken.getBytes(StandardCharsets.UTF_8)));
                    }
                    catch (SecurityException se) {
                        throw new RuntimeException(String.format(
                                "Could not decrypt token from v3io principal, name=%s; encrypted token=%s",
                                name, encryptedToken), se);
                    }
                    catch (Throwable t) {
                        throw new RuntimeException(String.format(
                                "Unexpected exception. Failed to parse principal, name=%s; encrypted token=%s",
                                name, encryptedToken), t);
                    }
                    return token;
                }
            }
            else {
                return name;
            }
        }
        throw new NoSuchElementException("No Principal present.");
    }

    private final class TokenHandler
    {
        // TODO: Consider moving key and associated data outside of the source code to make it a little bit more secure.
        private final byte[] associatedData = "iguazio".getBytes(StandardCharsets.UTF_8);
        private final byte[] salt = "Cheburashka".getBytes(StandardCharsets.UTF_8);
        private final String encryptionKey = "this key should be shared with presto-hive module in order to be able to decrypt the token";

        public String decrypt(byte[] encryptedToken)
        {
            final byte[] phrase =
                    new AesGcmEncryption().decrypt(toKey(encryptionKey, salt), encryptedToken, associatedData);

            return new String(phrase);
        }

        // Convert to 16 bytes key
        private byte[] toKey(String passPhrase, byte[] salt)
        {
            int iterationsCount = 1234;
            KeySpec spec = new PBEKeySpec(passPhrase.toCharArray(), salt, iterationsCount, 128);
            try {
                SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
                byte[] key = f.generateSecret(spec).getEncoded();
                return key;
            }
            catch (Exception e) {
                throw new SecurityException("could not encrypt", e);
            }
        }
    }

    @SuppressWarnings("WeakerAccess")
    private final class AesGcmEncryption
    {
        private static final String ALGORITHM = "AES/GCM/NoPadding";
        private static final int TAG_LENGTH_BIT = 128;
        private final Provider provider;
        private ThreadLocal<Cipher> cipherWrapper = new ThreadLocal<>();

        public AesGcmEncryption()
        {
            this(new SecureRandom(), null);
        }

        public AesGcmEncryption(SecureRandom secureRandom, Provider provider)
        {
            this.provider = provider;
        }

        public byte[] decrypt(byte[] rawEncryptionKey, byte[] encryptedData, byte[] associatedData)
                throws SecurityException
        {
            byte[] iv = null;
            byte[] encrypted = null;
            try {
                ByteBuffer byteBuffer = ByteBuffer.wrap(encryptedData);

                int ivLength = byteBuffer.get();
                iv = new byte[ivLength];
                byteBuffer.get(iv);
                encrypted = new byte[byteBuffer.remaining()];
                byteBuffer.get(encrypted);

                final Cipher cipherDec = getCipher();
                cipherDec.init(Cipher.DECRYPT_MODE, new SecretKeySpec(rawEncryptionKey, "AES"), new GCMParameterSpec(TAG_LENGTH_BIT, iv));
                if (associatedData != null) {
                    cipherDec.updateAAD(associatedData);
                }
                return cipherDec.doFinal(encrypted);
            }
            catch (Exception e) {
                throw new SecurityException("could not decrypt", e);
            }
            finally {
                wipe(iv);
                wipe(encrypted);
            }
        }

        private void wipe(byte[] bytes)
        {
            if (bytes != null) {
                SecureRandom random = new SecureRandom();
                random.nextBytes(bytes);
            }
        }

        private Cipher getCipher()
        {
            Cipher cipher = cipherWrapper.get();
            if (cipher == null) {
                try {
                    if (provider != null) {
                        cipher = Cipher.getInstance(ALGORITHM, provider);
                    }
                    else {
                        cipher = Cipher.getInstance(ALGORITHM);
                    }
                }
                catch (Exception e) {
                    throw new IllegalStateException("could not get cipher instance", e);
                }
                cipherWrapper.set(cipher);
                return cipherWrapper.get();
            }
            else {
                return cipher;
            }
        }
    }
}
