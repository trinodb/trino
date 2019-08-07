package io.prestosql.sql.util;

import java.util.ArrayList;
import java.util.List;

public class ArrayUtils<T> {

    public List<T> copyAndReverse(List<T> list) {
        if (list == null) {
            return null;
        }
        List<T> newArray = new ArrayList<>();
        for (int i = list.size() - 1; i >= 0; --i) {
            newArray.add(list.get(i));
        }
        return newArray;
    }
}
