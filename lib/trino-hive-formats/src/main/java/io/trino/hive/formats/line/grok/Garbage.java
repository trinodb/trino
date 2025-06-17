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
package io.trino.hive.formats.line.grok;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The Leon the professional of {@code Grok}.<br>
 * Garbage is use by grok to remove or rename elements before getting the final output
 *
 * @author anthonycorbacho
 * @since 0.0.2
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class Garbage
{
    private final List<String> toRemove;
    private final Map<String, Object> toRename;

    /**
     * Create a new {@code Garbage} object.
     */
    public Garbage()
    {
        toRemove = new ArrayList<String>();
        toRename = new TreeMap<String, Object>();
        // this is a default value to remove
        toRemove.add("UNWANTED");
    }

    /**
     * Remove from the map the unwilling items.
     *
     * @param map to clean
     * @return nb of deleted item
     */
    public int remove(Map<String, Object> map)
    {
        int item = 0;

        if (map == null || map.isEmpty()) {
            return item;
        }

        List<String> keysToRemove = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            for (String s : toRemove) {
                if (entry.getKey().equals(s)) {
                    keysToRemove.add(entry.getKey());
                    item++;
                }
            }
        }

        for (String s : keysToRemove) {
            map.remove(s);
        }
        return item;
    }

    /**
     * Rename the item from the map.
     *
     * @param map elem to rename
     * @return nb of renamed items
     */
    public int rename(Map<String, Object> map)
    {
        int item = 0;

        if (map == null || map.isEmpty() || toRename.isEmpty()) {
            return item;
        }

        for (Map.Entry<String, Object> entry : toRename.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                Object obj = map.remove(entry.getKey());
                map.put(entry.getValue().toString(), obj);
                item++;
            }
        }

        return item;
    }
}
