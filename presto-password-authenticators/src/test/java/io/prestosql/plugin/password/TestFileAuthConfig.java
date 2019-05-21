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
package io.prestosql.plugin.password;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.password.file.Credentials;
import io.prestosql.plugin.password.file.EncryptionUtil;
import io.prestosql.plugin.password.file.FileBasedAuthenticationConfig;
import io.prestosql.plugin.password.file.User;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static io.prestosql.plugin.password.file.HashingAlgorithm.BCRYPT;
import static io.prestosql.plugin.password.file.HashingAlgorithm.PBKDF2;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestFileAuthConfig
{
    private static final Logger log = Logger.get(TestFileAuthConfig.class);

    private int maximumSize = 1000;
    private Cache<Credentials, Boolean> bcryptCache;
    private Cache<Credentials, Boolean> pbkdf2Cache;
    private int bcryptMinCost = 8;
    private int pbkdf2MinIterations = 1000;
    private FileBasedAuthenticationConfig config;

    @BeforeMethod
    public void setUp()
    {
        config = new FileBasedAuthenticationConfig();
        config.setAuthTokenCacheStoreMaxSize(maximumSize);
        bcryptCache = CacheBuilder.newBuilder()
                .maximumSize(config.getAuthTokenCacheStoreMaxSize())
                .build();

        pbkdf2Cache = CacheBuilder.newBuilder()
                .maximumSize(config.getAuthTokenCacheStoreMaxSize())
                .build();
        config.setBcryptMinCost(bcryptMinCost);
        config.setPbkdf2MinIterations(pbkdf2MinIterations);
    }

    @AfterMethod
    public void tearDown()
    {
        bcryptCache = null;
        pbkdf2Cache = null;
        config = null;
    }

    /* Check whether the correct hashing algorithm can be identified*/
    @Test
    public void testHashingAlgoBCRYPT()
    {
        String password = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
        assertEquals(EncryptionUtil.checkAndGetHashing(password, config.getBcryptMinCost(), config.getPbkdf2MinIterations()), BCRYPT);
    }

    @Test
    public void testHashingAlgoPBKDF2()
    {
        String password = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";
        assertEquals(EncryptionUtil.checkAndGetHashing(password, config.getBcryptMinCost(), config.getPbkdf2MinIterations()), PBKDF2);
    }

    /* Check whether the hashed password are in the correct format: restrictions can be imposed on the min cost (for BCRYPT) or min iterations (for PBKDF2),
    this can be configured by the user in password-authenticator.properties
    For eg.

    file.bcrypt.min-cost=8

    file.pbkdf2.min-iterations=1000
    */
    @Test (expectedExceptions = PrestoException.class)
    public void testInvalidPasswordFormatBCRYPT()
    {
        bcryptMinCost = 8;
        config.setBcryptMinCost(bcryptMinCost);
        String password = "$2y$07$XxMSjoWesbX9s9LCD5Kp1OaFD/bcLUq0zoRCTsTNwjF6N/nwHVCVm"; // bcrypt password created with Cost = 7, --> "htpasswd -n -B -C 7 userbcrypt"
        EncryptionUtil.checkAndGetHashing(password, config.getBcryptMinCost(), config.getPbkdf2MinIterations()); // min enforced accepted bcrypt password creation cost = 8, hence Exception

        bcryptMinCost = 7;
        config.setBcryptMinCost(bcryptMinCost);
        assertEquals(EncryptionUtil.checkAndGetHashing(password, config.getBcryptMinCost(), config.getPbkdf2MinIterations()), BCRYPT);
    }

    @Test (expectedExceptions = PrestoException.class)
    public void testInvalidPasswordFormatPBKDF2()
    {
        pbkdf2MinIterations = 1000;
        config.setPbkdf2MinIterations(pbkdf2MinIterations);
        String password = "100:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0"; // pbkdf2 password with Iteration = 100
        EncryptionUtil.checkAndGetHashing(password, config.getBcryptMinCost(), config.getPbkdf2MinIterations()); // min enforced accepted pbkdf2 password creation iteration = 1000, hence Exception

        pbkdf2MinIterations = 100;
        config.setPbkdf2MinIterations(pbkdf2MinIterations);
        assertEquals(EncryptionUtil.checkAndGetHashing(password, config.getBcryptMinCost(), config.getPbkdf2MinIterations()), PBKDF2);
    }

    /* Checks for authenticate functionality
     */
    @Test
    public void testValidUsernamePasswordBCRYPT()
    {
        String storedUsername = "userbcrypt";
        String storedPassword = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa"; //Hashed version of "user123"

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());
        assertTrue(credential.authenticate("userbcrypt", "user123"));
    }

    @Test
    public void testInValidPasswordBCRYPT()
    {
        String storedUsername = "userbcrypt";
        String storedPassword = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        assertFalse(credential.authenticate("userbcrypt", "user12"));
        assertFalse(credential.authenticate("userbcrypt", ""));
    }

    @Test
    public void testInValidUsernameBCRYPT()
    {
        String storedUsername = "userbcrypt";
        String storedPassword = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        assertFalse(credential.authenticate("", "user123"));
        assertFalse(credential.authenticate("wronguser", "user123"));
    }

    @Test
    public void testValidUsernamePasswordPBKDF2()
    {
        String storedUsername = "userpbkdf2";
        String storedPassword = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0"; //Hashed version of "passsword"

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());
        assertTrue(credential.authenticate("userpbkdf2", "password"));
    }

    @Test
    public void testInValidPasswordPBKDF2()
    {
        String storedUsername = "userpbkdf2";
        String storedPassword = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        assertFalse(credential.authenticate("userpbkdf2", ""));
        assertFalse(credential.authenticate("userpbkdf2", "paswor"));
    }

    @Test
    public void testInValidUsernamePBKDF2()
    {
        String storedUsername = "userpbkdf2";
        String storedPassword = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        assertFalse(credential.authenticate("", "password"));
        assertFalse(credential.authenticate("uwronguser", "pasword"));
    }

    /* Impersonification */
    @Test
    public void testImpersonification()
    {
        String storedUsernameBCRYPT = "userbcrypt";
        String storedPasswordBCRYPT = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
        User credentialBCRYPT = new User(storedUsernameBCRYPT, storedPasswordBCRYPT, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        String storedUsernamePBKDF2 = "userpbkdf2";
        String storedPasswordPBKDF2 = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";
        User credentialPBKDF2 = new User(storedUsernamePBKDF2, storedPasswordPBKDF2, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        assertFalse(credentialBCRYPT.authenticate(credentialPBKDF2.getName(), credentialPBKDF2.getPassword()));
        assertFalse(credentialPBKDF2.authenticate(credentialBCRYPT.getName(), credentialBCRYPT.getPassword()));

        assertTrue(credentialBCRYPT.authenticate(credentialBCRYPT.getName(), "user123"));
        assertTrue(credentialPBKDF2.authenticate(credentialPBKDF2.getName(), "password"));
    }

    /* Test Caching Performance of the Auth tokens in LRU Cache */
    @Test
    public void testAuthStorePBKDF2Cache()
    {
        String storedUsername = "userpbkdf2" + Instant.now().toString();
        String storedPassword = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0"; //Hashed version of "passsword"

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        Instant previous = Instant.now();
        credential.authenticate(storedUsername, "password");
        Instant current = Instant.now();
        long initGap = ChronoUnit.MILLIS.between(previous, current);

        Instant previous1 = Instant.now();
        credential.authenticate(storedUsername, "password");
        Instant current1 = Instant.now();
        long gapAfterCaching = ChronoUnit.MILLIS.between(previous1, current1);
        assertTrue(gapAfterCaching < initGap);
    }

    @Test
    public void testAuthStoreBCRYPTCache()
    {
        String storedUsername = "userbcrypt" + Instant.now().toString();
        String storedPassword = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa"; //Hashed version of "user123"

        User credential = new User(storedUsername, storedPassword, bcryptCache, pbkdf2Cache, config.getBcryptMinCost(), config.getPbkdf2MinIterations());

        Instant previous = Instant.now();
        credential.authenticate(storedUsername, "user123");
        Instant current = Instant.now();
        long initGap = ChronoUnit.MILLIS.between(previous, current);

        Instant previous1 = Instant.now();
        credential.authenticate(storedUsername, "user123");
        Instant current1 = Instant.now();
        long gapAfterCaching = ChronoUnit.MILLIS.between(previous1, current1);

        assertTrue(gapAfterCaching < initGap);
    }

    @Test
    public void testFileAuthConfigs()
    {
        config = config.setConfigFile("file_auth");
        config = config.setRefreshPeriod(new Duration(5, TimeUnit.MILLISECONDS));
        config = config.setBcryptMinCost(8);
        config = config.setPbkdf2MinIterations(1000);

        assertTrue(config.getConfigFile().equals("file_auth"));
        assertFalse(config.getConfigFile().equals("file"));

        assertTrue(config.getRefreshPeriod().getValue() == 5);
        assertFalse(config.getRefreshPeriod().getValue() == 6);

        assertTrue(config.getBcryptMinCost() == 8);
        assertFalse(config.getBcryptMinCost() == 10);

        assertTrue(config.getPbkdf2MinIterations() == 1000);
        assertFalse(config.getPbkdf2MinIterations() == 1001);
    }
}
