package io.prestosql.plugin.bigo.udf;

import org.testng.annotations.Test;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class BigoStringFunctionsTest {

    @Test
    private void testStrPosAndPosition()
    {
        long pos = BigoStringFunctions.inStr(utf8Slice("bigolive"), utf8Slice("go"));

        assertEquals(pos, 3);
    }

}