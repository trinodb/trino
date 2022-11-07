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
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.jwt.JwkDecoder.JwkEcPublicKey;
import io.trino.server.security.jwt.JwkDecoder.JwkRsaPublicKey;
import org.testng.annotations.Test;

import java.io.File;
import java.security.Key;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECParameterSpec;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static io.trino.server.security.jwt.JwkDecoder.decodeKeys;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
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
                "      \"kty\": \"EC\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"crv\": \"P-256\",\n" +
                "      \"kid\": \"example-ec\",\n" +
                "      \"x\": \"W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y\",\n" +
                "      \"y\": \"XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg\",\n" +
                "      \"alg\": \"ES256\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 2);
        assertTrue(keys.get("example-rsa") instanceof JwkRsaPublicKey);
        assertTrue(keys.get("example-ec") instanceof JwkEcPublicKey);
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
                "    },\n" +
                "    {\n" +
                "      \"kty\": \"EC\",\n" +
                "      \"use\": \"sig\",\n" +
                "      \"crv\": \"P-256\",\n" +
                "      \"x\": \"W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y\",\n" +
                "      \"y\": \"XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg\",\n" +
                "      \"alg\": \"ES256\"\n" +
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

        RSAPublicKey expectedPublicKey = (RSAPublicKey) PemReader.loadPublicKey(new File(Resources.getResource("jwk/jwk-rsa-public.pem").toURI()));
        assertEquals(publicKey.getPublicExponent(), expectedPublicKey.getPublicExponent());
        assertEquals(publicKey.getModulus(), expectedPublicKey.getModulus());

        PrivateKey privateKey = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").toURI()), Optional.empty());
        String jwt = newJwtBuilder()
                .signWith(privateKey)
                .setHeaderParam(JwsHeader.KEY_ID, "test-rsa")
                .setSubject("test-user")
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        Jws<Claims> claimsJws = newJwtParserBuilder()
                .setSigningKeyResolver(new SigningKeyResolver()
                {
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
                .build()
                .parseClaimsJws(jwt);

        assertEquals(claimsJws.getBody().getSubject(), "test-user");
    }

    @Test
    public void testEcKey()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"kid\": \"test-ec\",\n" +
                "      \"kty\": \"EC\",\n" +
                "      \"crv\": \"P-256\",\n" +
                "      \"x\": \"W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y\",\n" +
                "      \"y\": \"XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 1);
        assertTrue(keys.get("test-ec") instanceof JwkEcPublicKey);
    }

    @Test
    public void testEcInvalidCurve()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"kid\": \"test-ec\",\n" +
                "      \"kty\": \"EC\",\n" +
                "      \"crv\": \"taco\",\n" +
                "      \"x\": \"W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y\",\n" +
                "      \"y\": \"XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testEcInvalidX()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"kid\": \"test-ec\",\n" +
                "      \"kty\": \"EC\",\n" +
                "      \"crv\": \"P-256\",\n" +
                "      \"x\": \"!!INVALID!!\",\n" +
                "      \"y\": \"XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testEcInvalidY()
    {
        Map<String, PublicKey> keys = decodeKeys("" +
                "{\n" +
                "  \"keys\": [\n" +
                "    {\n" +
                "      \"kid\": \"test-ec\",\n" +
                "      \"kty\": \"EC\",\n" +
                "      \"crv\": \"P-256\",\n" +
                "      \"x\": \"W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y\",\n" +
                "      \"y\": \"!!INVALID!!\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        assertEquals(keys.size(), 0);
    }

    @Test
    public void testJwtEc()
            throws Exception
    {
        assertJwtEc("jwk-ec-p256", EcCurve.P_256);
        assertJwtEc("jwk-ec-p384", EcCurve.P_384);
        assertJwtEc("jwk-ec-p512", EcCurve.P_521);
    }

    private static void assertJwtEc(String keyName, ECParameterSpec expectedSpec)
            throws Exception
    {
        String jwkKeys = Resources.toString(Resources.getResource("jwk/jwk-public.json"), UTF_8);
        Map<String, PublicKey> keys = decodeKeys(jwkKeys);

        ECPublicKey publicKey = (ECPublicKey) keys.get(keyName);
        assertNotNull(publicKey);

        assertSame(publicKey.getParams(), expectedSpec);

        ECPublicKey expectedPublicKey = (ECPublicKey) PemReader.loadPublicKey(new File(Resources.getResource("jwk/" + keyName + "-public.pem").toURI()));
        assertEquals(publicKey.getW(), expectedPublicKey.getW());
        assertEquals(publicKey.getParams().getCurve(), expectedPublicKey.getParams().getCurve());
        assertEquals(publicKey.getParams().getGenerator(), expectedPublicKey.getParams().getGenerator());
        assertEquals(publicKey.getParams().getOrder(), expectedPublicKey.getParams().getOrder());
        assertEquals(publicKey.getParams().getCofactor(), expectedPublicKey.getParams().getCofactor());

        PrivateKey privateKey = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/" + keyName + "-private.pem").toURI()), Optional.empty());
        String jwt = newJwtBuilder()
                .signWith(privateKey)
                .setHeaderParam(JwsHeader.KEY_ID, keyName)
                .setSubject("test-user")
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        Jws<Claims> claimsJws = newJwtParserBuilder()
                .setSigningKeyResolver(new SigningKeyResolver()
                {
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
                        assertEquals(keyId, keyName);
                        return publicKey;
                    }
                })
                .build()
                .parseClaimsJws(jwt);

        assertEquals(claimsJws.getBody().getSubject(), "test-user");
    }
}
