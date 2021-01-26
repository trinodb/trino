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
package io.trino.spiller;

import io.trino.spi.TrinoException;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.expectThrows;

public class TestAesSpillCipher
{
    @Test
    public void test()
    {
        AesSpillCipher spillCipher = new AesSpillCipher();
        // test [0, 257] buffer sizes to check off all padding cases
        for (int size = 0; size <= 257; size++) {
            byte[] data = randomBytes(size);
            // .clone() to prevent cipher from modifying the content we assert against
            byte[] encrypted = encryptExact(spillCipher, data.clone());
            assertEquals(data, decryptExact(spillCipher, encrypted));
        }
        // verify that initialization vector is not re-used
        assertNotEquals(encryptExact(spillCipher, new byte[0]), encryptExact(spillCipher, new byte[0]), "IV values must not be reused");

        byte[] encrypted = encryptExact(spillCipher, randomBytes(1));
        spillCipher.close();
        spillCipher.close(); // should not throw an exception

        assertFailure(() -> decryptExact(spillCipher, encrypted), "Spill cipher already closed");
        assertFailure(() -> encryptExact(spillCipher, randomBytes(1)), "Spill cipher already closed");
    }

    private static byte[] encryptExact(SpillCipher cipher, byte[] data)
    {
        byte[] output = new byte[cipher.encryptedMaxLength(data.length)];
        int outLength = cipher.encrypt(data, 0, data.length, output, 0);
        if (output.length == outLength) {
            return output;
        }
        else {
            return Arrays.copyOfRange(output, 0, outLength);
        }
    }

    private static byte[] decryptExact(SpillCipher cipher, byte[] encryptedData)
    {
        byte[] output = new byte[cipher.decryptedMaxLength(encryptedData.length)];
        int outLength = cipher.decrypt(encryptedData, 0, encryptedData.length, output, 0);
        if (outLength == output.length) {
            return output;
        }
        else {
            return Arrays.copyOfRange(output, 0, outLength);
        }
    }

    private static void assertFailure(ThrowingRunnable runnable, String expectedErrorMessage)
    {
        TrinoException exception = expectThrows(TrinoException.class, runnable);
        assertEquals(exception.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertEquals(exception.getMessage(), expectedErrorMessage);
    }

    private static byte[] randomBytes(int size)
    {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
