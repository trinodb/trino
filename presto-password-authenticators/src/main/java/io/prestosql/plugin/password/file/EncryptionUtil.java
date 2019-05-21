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
package io.prestosql.plugin.password.file;

import at.favre.lib.crypto.bcrypt.BCrypt;
import at.favre.lib.crypto.bcrypt.IllegalBCryptFormatException;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;

import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;

public class EncryptionUtil
{
    private static final Logger log = Logger.get(EncryptionUtil.class);

    private EncryptionUtil() {}

    // enforce minumum cost in for BCrypt Passwords : Min value default: 8
    public static boolean isBCryptCostValid(String password, int minCost)
    {
        try {
            BCrypt.HashData hashData = BCrypt.Version.VERSION_2A.parser.parse(password.getBytes());
            return hashData.cost >= minCost;
        }
        catch (IllegalBCryptFormatException e) {
            throw new PrestoException(CONFIGURATION_INVALID, "Invalid BCrypt Password", e);
        }
    }

    // enforce minimum iterations for PBJKDF2 passwords : Min Value defult: 1000
    public static boolean isPBKDF2CostValid(String password, int minCost)
    {
        List<String> pass = Splitter.on(":").limit(2).splitToList(password);
        return Integer.parseInt(pass.get(0)) >= minCost;
    }

    public static boolean isBCryptPasswordValid(String inputPassword, String hashedPassword)
    {
        return BCrypt.verifyer().verify(inputPassword.toCharArray(), hashedPassword).verified;
    }

    public static boolean isPBKDF2PasswordValid(String inputPassword, String hashedPassword)
    {
        if (!hashedPassword.contains(":")) {
            throw new HashedPasswordException("Invalid PBKDF2 Password");
        }
        String[] parts = hashedPassword.split(":");
        int iterations = Integer.parseInt(parts[0]);

        try {
            byte[] salt = fromHex(parts[1]);
            byte[] hash = fromHex(parts[2]);

            PBEKeySpec spec = new PBEKeySpec(inputPassword.toCharArray(), salt, iterations, hash.length * 8);
            SecretKeyFactory key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            byte[] inputHash = key.generateSecret(spec).getEncoded();
            int diff = 0;
            diff = hash.length ^ inputHash.length;
            if (diff != 0) { //immediate error if the hash length doesn't match
                throw new PrestoException(CONFIGURATION_INVALID, "Invalid PBKDF2 password");
            }
            for (int i = 0; i < hash.length && i < inputHash.length; i++) {
                diff |= hash[i] ^ inputHash[i];
            }
            return diff == 0;
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new PrestoException(CONFIGURATION_INVALID, "Invalid PBKDF2 password", e);
        }
    }

    private static byte[] fromHex(String hex)
    {
        return BaseEncoding.base16().lowerCase().decode(hex);
    }

    public static HashingAlgorithm checkAndGetHashing(String password, int bcryptMinCost, int pbkdf2MinIterations)
            throws HashedPasswordException
    {
        if (password.startsWith("$2y")) {
            if (!isBCryptCostValid(password, bcryptMinCost)) {
                throw new HashedPasswordException("Minimum cost of bcrypt password should be " + bcryptMinCost);
            }
            return HashingAlgorithm.BCRYPT;
        }

        if (password.contains(":")) {
            if (!isPBKDF2CostValid(password, pbkdf2MinIterations)) {
                throw new HashedPasswordException("Minimum Iteration of PBKDF2 Password should be " + pbkdf2MinIterations);
            }
            return HashingAlgorithm.PBKDF2;
        }

        throw new HashedPasswordException("Hashing Algorithm cannot be determined");
    }
}
