package org.apache.doris.udf.map;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MapEntriesUdf extends UDF {

    public ArrayList<ArrayList<String>> evaluate(HashMap<String, String> map) {
        ArrayList<ArrayList<String>> entries = new ArrayList<>();
        if (map!= null) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                ArrayList<String> row = new ArrayList<>();
                String key = entry.getKey();
                row.add(key);
                String value = entry.getValue();
                row.add(value);
                entries.add(row);
            }
        }
        return entries;
    }
}
