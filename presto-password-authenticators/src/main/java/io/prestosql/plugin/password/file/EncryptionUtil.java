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

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Iterator;
import java.util.List;

import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EncryptionUtil
{
    private static final int BCRYPT_MIN_COST = 8;
    private static final int PBKDF2_MIN_ITERATIONS = 1000;

    private EncryptionUtil() {}

    public static int getBCryptCost(String password)
    {
        try {
            return BCrypt.Version.VERSION_2A.parser.parse(password.getBytes(UTF_8)).cost;
        }
        catch (IllegalBCryptFormatException e) {
            throw new HashedPasswordException("Invalid BCrypt password", e);
        }
    }

    public static int getPBKDF2Iterations(String password)
    {
        Iterator<String> parts = Splitter.on(":").split(password).iterator();
        try {
            return Integer.parseInt(parts.next());
        }
        catch (NumberFormatException e) {
            throw new HashedPasswordException("Invalid PBKDF2 password", e);
        }
    }

    public static boolean isBCryptPasswordValid(String inputPassword, String hashedPassword)
    {
        return BCrypt.verifyer().verify(inputPassword.toCharArray(), hashedPassword).verified;
    }

    public static boolean isPBKDF2PasswordValid(String inputPassword, String hashedPassword)
    {
        List<String> parts = Splitter.on(":").splitToList(hashedPassword);
        if (parts.size() != 3) {
            throw new HashedPasswordException("Invalid PBKDF2 Password");
        }

        int iterations = Integer.parseInt(parts.get(0));
        byte[] salt = base16().lowerCase().decode(parts.get(1));
        byte[] hash = base16().lowerCase().decode(parts.get(2));

        try {
            KeySpec spec = new PBEKeySpec(inputPassword.toCharArray(), salt, iterations, hash.length * 8);
            SecretKeyFactory key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            byte[] inputHash = key.generateSecret(spec).getEncoded();

            if (hash.length != inputHash.length) {
                throw new HashedPasswordException("PBKDF2 password input is malformed");
            }
            return MessageDigest.isEqual(hash, inputHash);
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new HashedPasswordException("Invalid PBKDF2 password", e);
        }
    }

    public static HashingAlgorithm getHashingAlgorithm(String password)
    {
        if (password.startsWith("$2y")) {
            if (getBCryptCost(password) < BCRYPT_MIN_COST) {
                throw new HashedPasswordException("Minimum cost of BCrypt password must be " + BCRYPT_MIN_COST);
            }
            return HashingAlgorithm.BCRYPT;
        }

        if (password.contains(":")) {
            if (getPBKDF2Iterations(password) < PBKDF2_MIN_ITERATIONS) {
                throw new HashedPasswordException("Minimum iterations of PBKDF2 password must be " + PBKDF2_MIN_ITERATIONS);
            }
            return HashingAlgorithm.PBKDF2;
        }

        throw new HashedPasswordException("Password hashing algorithm cannot be determined");
    }
}
