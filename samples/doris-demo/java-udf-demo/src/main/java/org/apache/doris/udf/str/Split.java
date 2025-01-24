package org.apache.doris.udf.str;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Split extends UDF {

    /**
     * split(String, String)
     * 
     * SELECT split('a:1,b:2,c:3', ',');
     * 
     * @param input
     * @param entryDelimiter
     * @return
     */
    public ArrayList<String> evaluate(String str, String delimiter) {
        if (str == null || delimiter == null) {
            return null;
        }

        ArrayList<String> result = new ArrayList<>();
        int start = 0;
        int end;
        while ((end = str.indexOf(delimiter, start)) != -1) {
            result.add(str.substring(start, end));
            start = end + delimiter.length();
        }
        result.add(str.substring(start));

        return result;
    }
}
