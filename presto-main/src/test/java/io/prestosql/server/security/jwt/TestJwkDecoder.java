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

import com.google.common.io.Resources;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SigningKeyResolver;
import io.prestosql.server.security.jwt.JwkDecoder.JwkRsaPublicKey;
import org.testng.annotations.Test;

import java.io.File;
import java.security.Key;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.server.security.jwt.JwkDecoder.decodeKeys;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestJwkDecoder
{
    @Test
    public void testReadRsaKeys()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"e\": \"AQAB\",\n" +
                "      \"n\": \"mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"kty\": \"RSA\",\n" +
                "      \"kid\": \"example-rsa\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"kid\": \"other-rsa\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"n\": \"teG3wvigoU_KPbPAiEVERFmlGeHWPsnqbEk1pAhz69B0kGHJXU8l8tPHpTw0Gy_M9BJ5WAe9FvXL41xSFbqMGiJ7DIZ32ejlncrf2vGkMl26C5p8OOvuS6ThFjREUzWbV0sYtJL0nNjzmQNCQeb90tDQDZW229ZeUNlM2yN0QRisKlGFSK7uL8X0dRUbXnfgS6eI4mvSAK6tqq3n8IcPA0PxBr-R81rtdG70C2zxlPQ4Wp_MJzjb81d-RPdcYd64loOMhhHFbbfq2bTS9TSn_Y16lYA7gyRGSPhwcsdqOH2qqon7QOiF8gtrvztwd9TpxecPd7mleGGWVFlN6pTQYQ\",\n" +
                "      \"kty\": \"RSA\",\n" +
                "      \"e\": \"AQAB\",\n" +
                "      \"use\": \"sig\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 2);
        assertTrue(keys.get("example-rsa") instanceof JwkRsaPublicKey);
        assertTrue(keys.get("other-rsa") instanceof JwkRsaPublicKey);
    }

    @Test
    public void testReadEcKey()
    {
        // EC keys are currently ignored
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"kty\" : \"EC\",\n" +
                "      \"crv\" : \"P-256\",\n" +
                "      \"x\"   : \"SVqB4JcUD6lsfvqMr-OKUNUphdNn64Eay60978ZlL74\",\n" +
                "      \"y\"   : \"lf0u0pMj4lGAzZix5u4Cm5CMQIgMNpkwy163wtKYVKI\",\n" +
                "      \"d\"   : \"0g5vAEKzugrXaRbgKG0Tj2qJ5lMP4Bezds1_sTybkfk\"\n" +
                "    }" +
                "  ]\n" +
                "}");
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testNoKeyId()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"e\": \"AQAB\",\n" +
                "      \"n\": \"mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"kty\": \"RSA\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testRsaNoModulus()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"e\": \"AQAB\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"kty\": \"RSA\",\n" +
                "      \"kid\": \"2c6fa6f5950a7ce465fcf247aa0b094828ac952c\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testRsaNoExponent()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"n\": \"mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"kty\": \"RSA\",\n" +
                "      \"kid\": \"2c6fa6f5950a7ce465fcf247aa0b094828ac952c\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testRsaInvalidModulus()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"e\": \"AQAB\",\n" +
                "      \"n\": \"!!INVALID!!\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"kty\": \"RSA\",\n" +
                "      \"kid\": \"2c6fa6f5950a7ce465fcf247aa0b094828ac952c\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testRsaInvalidExponent()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"e\": \"!!INVALID!!\",\n" +
                "      \"n\": \"mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ\",\n" +
                "      \"alg\": \"RS256\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"kty\": \"RSA\",\n" +
                "      \"kid\": \"2c6fa6f5950a7ce465fcf247aa0b094828ac952c\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testJwtRsa()
            throws Exception
    {
        String jwkKeys = Resources.toString(Resources.getResource("jwk/jwk-public.json"), UTF_8);
        Map<String, PublicKey> keys = decodeKeys(jwkKeys);

        RSAPublicKey publicKey = (RSAPublicKey) keys.get("test-rsa");
        assertNotNull(publicKey);

        RSAPublicKey expectedPublicKey = (RSAPublicKey) PemReader.loadPublicKey(new File(Resources.getResource("jwk/jwk-rsa-public.pem").getPath()));
        assertEquals(publicKey.getPublicExponent(), expectedPublicKey.getPublicExponent());
        assertEquals(publicKey.getModulus(), expectedPublicKey.getModulus());

        PrivateKey privateKey = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").getPath()), Optional.empty());
        String jwt = Jwts.builder()
                .signWith(SignatureAlgorithm.RS256, privateKey)
                .setHeaderParam(JwsHeader.KEY_ID, "test-rsa")
                .setSubject("test-user")
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        Jws<Claims> claimsJws = Jwts.parser()
                .setSigningKeyResolver(new SigningKeyResolver() {
                    @Override
                    public Key resolveSigningKey(JwsHeader header, Claims claims)
                    {
                        return getKey(header);
                    }

                    @Override
                    public Key resolveSigningKey(JwsHeader header, String plaintext)
                    {
                        return getKey(header);
                    }

                    private Key getKey(JwsHeader<?> header)
                    {
                        String keyId = header.getKeyId();
                        assertEquals(keyId, "test-rsa");
                        return publicKey;
                    }
                })
                .parseClaimsJws(jwt);

        assertEquals(claimsJws.getBody().getSubject(), "test-user");
    }
}
