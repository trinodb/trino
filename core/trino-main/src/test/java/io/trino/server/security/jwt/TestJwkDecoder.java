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
import io.jsonwebtoken.LocatorAdapter;
import io.trino.server.security.jwt.JwkDecoder.JwkEcPublicKey;
import io.trino.server.security.jwt.JwkDecoder.JwkRsaPublicKey;
import org.junit.jupiter.api.Test;

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
import static org.assertj.core.api.Assertions.assertThat;

public class TestJwkDecoder
{
    @Test
    public void testReadRsaKeys()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "e": "AQAB",
                      "n": "mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ",
                      "alg": "RS256",
                      "use": "sig",
                      "kty": "RSA",
                      "kid": "example-rsa"
                    },
                    {
                      "kty": "EC",
                      "use": "sig",
                      "crv": "P-256",
                      "kid": "example-ec",
                      "x": "W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y",
                      "y": "XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg",
                      "alg": "ES256"
                    }
                  ]
                }
                """);
        assertThat(keys).hasSize(2);
        assertThat(keys.get("example-rsa")).isInstanceOf(JwkRsaPublicKey.class);
        assertThat(keys.get("example-ec")).isInstanceOf(JwkEcPublicKey.class);
    }

    @Test
    public void testNoKeyId()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "e": "AQAB",
                      "n": "mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ",
                      "alg": "RS256",
                      "use": "sig",
                      "kty": "RSA"
                    },
                    {
                      "kty": "EC",
                      "use": "sig",
                      "crv": "P-256",
                      "x": "W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y",
                      "y": "XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg",
                      "alg": "ES256"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testRsaNoModulus()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "e": "AQAB",
                      "alg": "RS256",
                      "use": "sig",
                      "kty": "RSA",
                      "kid": "2c6fa6f5950a7ce465fcf247aa0b094828ac952c"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testRsaNoExponent()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "n": "mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ",
                      "alg": "RS256",
                      "use": "sig",
                      "kty": "RSA",
                      "kid": "2c6fa6f5950a7ce465fcf247aa0b094828ac952c"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testRsaInvalidModulus()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "e": "AQAB",
                      "n": "!!INVALID!!",
                      "alg": "RS256",
                      "use": "sig",
                      "kty": "RSA",
                      "kid": "2c6fa6f5950a7ce465fcf247aa0b094828ac952c"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testRsaInvalidExponent()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "e": "!!INVALID!!",
                      "n": "mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ",
                      "alg": "RS256",
                      "use": "sig",
                      "kty": "RSA",
                      "kid": "2c6fa6f5950a7ce465fcf247aa0b094828ac952c"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testJwtRsa()
            throws Exception
    {
        String jwkKeys = Resources.toString(Resources.getResource("jwk/jwk-public.json"), UTF_8);
        Map<String, PublicKey> keys = decodeKeys(jwkKeys);

        RSAPublicKey publicKey = (RSAPublicKey) keys.get("test-rsa");
        assertThat(publicKey).isNotNull();

        RSAPublicKey expectedPublicKey = (RSAPublicKey) PemReader.loadPublicKey(new File(Resources.getResource("jwk/jwk-rsa-public.pem").toURI()));
        assertThat(publicKey.getPublicExponent()).isEqualTo(expectedPublicKey.getPublicExponent());
        assertThat(publicKey.getModulus()).isEqualTo(expectedPublicKey.getModulus());

        PrivateKey privateKey = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").toURI()), Optional.empty());
        String jwt = newJwtBuilder()
                .signWith(privateKey)
                .header().keyId("test-rsa").and()
                .subject("test-user")
                .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        Jws<Claims> claimsJws = newJwtParserBuilder()
                .keyLocator(new LocatorAdapter<>()
                {
                    @Override
                    protected Key locate(JwsHeader header)
                    {
                        String keyId = header.getKeyId();
                        assertThat(keyId).isEqualTo("test-rsa");
                        return publicKey;
                    }
                })
                .build()
                .parseSignedClaims(jwt);

        assertThat(claimsJws.getPayload().getSubject()).isEqualTo("test-user");
    }

    @Test
    public void testEcKey()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "kid": "test-ec",
                      "kty": "EC",
                      "crv": "P-256",
                      "x": "W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y",
                      "y": "XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg"
                    }
                  ]
                }
                """);
        assertThat(keys).hasSize(1);
        assertThat(keys.get("test-ec")).isInstanceOf(JwkEcPublicKey.class);
    }

    @Test
    public void testEcInvalidCurve()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "kid": "test-ec",
                      "kty": "EC",
                      "crv": "taco",
                      "x": "W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y",
                      "y": "XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testEcInvalidX()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "kid": "test-ec",
                      "kty": "EC",
                      "crv": "P-256",
                      "x": "!!INVALID!!",
                      "y": "XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
    }

    @Test
    public void testEcInvalidY()
    {
        Map<String, PublicKey> keys = decodeKeys(
                """
                {
                  "keys": [
                    {
                      "kid": "test-ec",
                      "kty": "EC",
                      "crv": "P-256",
                      "x": "W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y",
                      "y": "!!INVALID!!"
                    }
                  ]
                }
                """);
        assertThat(keys).isEmpty();
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
        assertThat(publicKey).isNotNull();

        assertThat(publicKey.getParams()).isSameAs(expectedSpec);

        ECPublicKey expectedPublicKey = (ECPublicKey) PemReader.loadPublicKey(new File(Resources.getResource("jwk/" + keyName + "-public.pem").toURI()));
        assertThat(publicKey.getW()).isEqualTo(expectedPublicKey.getW());
        assertThat(publicKey.getParams().getCurve()).isEqualTo(expectedPublicKey.getParams().getCurve());
        assertThat(publicKey.getParams().getGenerator()).isEqualTo(expectedPublicKey.getParams().getGenerator());
        assertThat(publicKey.getParams().getOrder()).isEqualTo(expectedPublicKey.getParams().getOrder());
        assertThat(publicKey.getParams().getCofactor()).isEqualTo(expectedPublicKey.getParams().getCofactor());

        PrivateKey privateKey = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/" + keyName + "-private.pem").toURI()), Optional.empty());
        String jwt = newJwtBuilder()
                .signWith(privateKey)
                .header().keyId(keyName).and()
                .subject("test-user")
                .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        Jws<Claims> claimsJws = newJwtParserBuilder()
                .keyLocator(new LocatorAdapter<>()
                {
                    @Override
                    protected Key locate(JwsHeader header)
                    {
                        String keyId = header.getKeyId();
                        assertThat(keyId).isEqualTo(keyName);
                        return publicKey;
                    }
                })
                .build()
                .parseSignedClaims(jwt);

        assertThat(claimsJws.getPayload().getSubject()).isEqualTo("test-user");
    }
}
