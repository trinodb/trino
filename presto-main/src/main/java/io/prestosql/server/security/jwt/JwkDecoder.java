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
package io.prestosql.server.security.jwt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.math.BigInteger;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Base64.getUrlDecoder;
import static java.util.Objects.requireNonNull;

public final class JwkDecoder
{
    private static final Logger log = Logger.get(JwkDecoder.class);
    private static final JsonCodec<Keys> KEYS_CODEC = JsonCodec.jsonCodec(Keys.class);

    private JwkDecoder() {}

    public static Map<String, PublicKey> decodeKeys(String jwkJson)
    {
        Keys keys = KEYS_CODEC.fromJson(jwkJson);
        return keys.getKeys().stream()
                .map(JwkDecoder::tryDecodeRsaKey)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableMap(JwkPublicKey::getKeyId, Function.identity()));
    }

    public static Optional<JwkRsaPublicKey> tryDecodeRsaKey(Map<String, String> properties)
    {
        String keyType = properties.get("kty");
        if (!"RSA".equals(keyType)) {
            // ignore non RSA keys
            return Optional.empty();
        }

        String keyId = properties.get("kid");
        if (Strings.isNullOrEmpty(keyId)) {
            // key id is required to index the key
            return Optional.empty();
        }

        // alg field is optional so not verified
        // use field is optional so not verified

        String encodedModulus = properties.get("n");
        if (Strings.isNullOrEmpty(encodedModulus)) {
            log.error("JWK RSA key %s does not contain the required modulus field 'n'", keyId);
            return Optional.empty();
        }
        String encodedExponent = properties.get("e");
        if (Strings.isNullOrEmpty(encodedExponent)) {
            log.error("JWK RSA key %s does not contain the required exponent field 'e'", keyId);
            return Optional.empty();
        }

        Optional<BigInteger> modulus = decodeBigint(keyId, "modulus", encodedModulus);
        if (modulus.isEmpty()) {
            return Optional.empty();
        }
        Optional<BigInteger> exponent = decodeBigint(keyId, "exponent", encodedExponent);
        if (exponent.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new JwkRsaPublicKey(keyId, exponent.get(), modulus.get()));
    }

    private static Optional<BigInteger> decodeBigint(String keyId, String fieldName, String encodedNumber)
    {
        try {
            return Optional.of(new BigInteger(1, getUrlDecoder().decode(encodedNumber)));
        }
        catch (IllegalArgumentException e) {
            log.error(e, "JWK %s %s is not a valid number", keyId, fieldName);
            return Optional.empty();
        }
    }

    public interface JwkPublicKey
            extends PublicKey
    {
        String getKeyId();
    }

    public static class JwkRsaPublicKey
            implements JwkPublicKey, RSAPublicKey
    {
        private final String keyId;
        private final BigInteger modulus;
        private final BigInteger exponent;

        public JwkRsaPublicKey(String keyId, BigInteger exponent, BigInteger modulus)
        {
            this.keyId = requireNonNull(keyId, "keyId is null");
            this.exponent = requireNonNull(exponent, "exponent is null");
            this.modulus = requireNonNull(modulus, "modulus is null");
        }

        @Override
        public String getKeyId()
        {
            return keyId;
        }

        @Override
        public BigInteger getModulus()
        {
            return modulus;
        }

        @Override
        public BigInteger getPublicExponent()
        {
            return exponent;
        }

        @Override
        public String getAlgorithm()
        {
            return "RSA";
        }

        @Override
        public String getFormat()
        {
            return "JWK";
        }

        @Override
        public byte[] getEncoded()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class Keys
    {
        private final List<Map<String, String>> keys;

        @JsonCreator
        public Keys(@JsonProperty("keys") List<Map<String, String>> keys)
        {
            this.keys = ImmutableList.copyOf(requireNonNull(keys, "keys is null"));
        }

        public List<Map<String, String>> getKeys()
        {
            return keys;
        }
    }
}
