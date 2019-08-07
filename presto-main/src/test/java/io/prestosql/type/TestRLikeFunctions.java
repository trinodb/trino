package io.prestosql.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author tangyun@bigo.sg
 * @date 8/5/19 3:14 PM
 */
public class TestRLikeFunctions {

    @Test
    public void testLikeVarchar() {

        assertEquals(RLikeFunctions.likeVarchar(getSlice("000fooooooooooooooooo"),
                getSlice("foo")), true);
        assertEquals(RLikeFunctions.likeVarchar(getSlice("fooooooooooooooooo"),
                getSlice("foo")), true);
        assertEquals(RLikeFunctions.likeVarchar(getSlice("foo"),
                getSlice("foo")), true);
        assertEquals(RLikeFunctions.likeVarchar(getSlice("1234567asdadas"),
                getSlice("[0-9]+[a-z]+")), true);
        assertEquals(RLikeFunctions.likeVarchar(getSlice("as_121skdls@ss.com"),
                getSlice("([0-9]|[a-z]|[a-z]|_)+@([a-z]|[0-9]|[A-Z])+.(com|cn|org)")), true);
        assertEquals(RLikeFunctions.likeVarchar(getSlice("000000"),
                getSlice("foo")), false);
    }

    public static Slice getSlice(String s) {
        if (s == null) {
            return Slices.allocate(0);
        }
        return Slices.wrappedBuffer(s.getBytes());
    }
}