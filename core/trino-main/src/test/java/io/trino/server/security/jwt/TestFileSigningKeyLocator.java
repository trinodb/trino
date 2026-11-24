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
package io.trino.server.security.jwt;

import com.google.common.io.Resources;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.SecurityException;
import io.jsonwebtoken.security.SignatureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Optional;

import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFileSigningKeyLocator
{
    @Test
    void testKeyIdSelectsKeyFile(@TempDir Path tempDir)
            throws IOException
    {
        SecretKey firstKey = writeHmacKey(tempDir.resolve("first.key"));
        SecretKey secondKey = writeHmacKey(tempDir.resolve("second.key"));
        FileSigningKeyLocator locator = new FileSigningKeyLocator(tempDir.resolve("${KID}.key").toString());

        assertThat(parseSubject(locator, createJwt(firstKey, "first"))).isEqualTo("test-user");
        assertThat(parseSubject(locator, createJwt(secondKey, "second"))).isEqualTo("test-user");
        assertThatThrownBy(() -> parseSubject(locator, createJwt(secondKey, "first")))
                .isInstanceOf(SignatureException.class);
        assertThatThrownBy(() -> parseSubject(locator, createJwt(firstKey, "unknown")))
                .isInstanceOf(SecurityException.class)
                .hasMessage("Unknown signing key ID");
    }

    @Test
    void testMissingKeyIdUsesDefaultKey(@TempDir Path tempDir)
            throws IOException
    {
        SecretKey key = writeHmacKey(tempDir.resolve("default-key.key"));
        FileSigningKeyLocator locator = new FileSigningKeyLocator(tempDir.resolve("${KID}.key").toString());

        assertThat(parseSubject(locator, createJwt(key, null))).isEqualTo("test-user");
    }

    @Test
    void testKeyIdCannotEscapeKeyDirectory(@TempDir Path tempDir)
            throws IOException
    {
        Path keyDirectory = Files.createDirectory(tempDir.resolve("keys"));
        SecretKey outsideKey = writeHmacKey(tempDir.resolve("outside.key"));
        FileSigningKeyLocator locator = new FileSigningKeyLocator(keyDirectory.resolve("${KID}.key").toString());

        assertThatThrownBy(() -> parseSubject(locator, createJwt(outsideKey, "../outside")))
                .isInstanceOf(SecurityException.class)
                .hasMessage("Unknown signing key ID");
    }

    @Test
    void testPublicKeyRejectsHmacSignedJwt()
            throws Exception
    {
        FileSigningKeyLocator locator = new FileSigningKeyLocator(Resources.getResource("jwk/jwk-rsa-public.pem").getPath());
        PrivateKey privateKey = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").toURI()), Optional.empty());

        assertThat(parseSubject(locator, createJwt(privateKey, null))).isEqualTo("test-user");
        assertThatThrownBy(() -> parseSubject(locator, createJwt(Jwts.SIG.HS256.key().build(), null)))
                .isInstanceOf(UnsupportedJwtException.class)
                .hasMessageEndingWith("but no HMAC key is configured");
    }

    private static SecretKey writeHmacKey(Path path)
            throws IOException
    {
        SecretKey key = Jwts.SIG.HS256.key().build();
        Files.writeString(path, Base64.getEncoder().encodeToString(key.getEncoded()));
        return key;
    }

    private static String createJwt(Key signingKey, String keyId)
    {
        return newJwtBuilder()
                .signWith(signingKey)
                .header().keyId(keyId).and()
                .subject("test-user")
                .compact();
    }

    private static String parseSubject(FileSigningKeyLocator locator, String jwt)
    {
        return newJwtParserBuilder()
                .keyLocator(locator)
                .build()
                .parseSignedClaims(jwt)
                .getPayload()
                .getSubject();
    }
}
