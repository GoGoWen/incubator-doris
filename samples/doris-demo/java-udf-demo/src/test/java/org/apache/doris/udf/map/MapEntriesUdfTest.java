package org.apache.doris.udf.map;

import org.apache.hadoop.hive.ql.exec.UDF;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MapEntriesUdfTest extends UDF {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testEmptyMap() {
        MapEntriesUdf udf = new MapEntriesUdf();
        HashMap<String, String> emptyMap = new HashMap<>();
        ArrayList<ArrayList<String>> result = udf.evaluate(emptyMap);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testNullKeyAndValues() {
        MapEntriesUdf udf = new MapEntriesUdf();
        HashMap<String, String> map = new HashMap<>();
        map.put(null, null);
        map.put("", "");
        map.put("key3", null);
        map.put("key4", "");

        ArrayList<ArrayList<String>> result = udf.evaluate(map);

        // 验证结果
        Assertions.assertEquals(4, result.size());
        Assertions.assertTrue(
            result.stream().anyMatch(e ->
                e.get(0) == null && e.get(1) == null
            )
        );
        Assertions.assertTrue(
            result.stream().anyMatch(e ->
                "".equals(e.get(0)) && "".equals(e.get(1))
            )
        );
        Assertions.assertTrue(
            result.stream().anyMatch(e ->
                "key3".equals(e.get(0)) && e.get(1) == null
            )
        );
        Assertions.assertTrue(
            result.stream().anyMatch(e ->
                "key4".equals(e.get(0)) && "".equals(e.get(1))
            )
        );
    }

    @Test
    public void testSpecialCharacters() {
        MapEntriesUdf udf = new MapEntriesUdf();
        HashMap<String, String> map = new HashMap<>();
        String expectedKey1 = "key=with=equals";
        String expectedValue1 = "value,with,commas";
        String expectedKey2 = "brace{key";
        String expectedValue2 = "value}withbrace";

        map.put(expectedKey1, expectedValue1);
        map.put(expectedKey2, expectedValue2);

        ArrayList<ArrayList<String>> result = udf.evaluate(map);

        // 验证结果
        Assertions.assertEquals(2, result.size());

        // 使用流式查找替代直接contains
        Assertions.assertTrue(
            result.stream().anyMatch(e ->
                expectedKey1.equals(e.get(0)) &&
                    expectedValue1.equals(e.get(1))
            )
        );
        Assertions.assertTrue(
            result.stream().anyMatch(e ->
                expectedKey2.equals(e.get(0)) &&
                    expectedValue2.equals(e.get(1))
            )
        );
    }

    @Test
    public void testNestedJsonValues() {
        MapEntriesUdf udf = new MapEntriesUdf();
        HashMap<String, String> map = new HashMap<>();

        String expectedKey1 = "ext";
        String expectedJson1 = "{\"eval_ab\":\"B\",\"nums\":[1,2,3]}"; // 注意无空格
        map.put(expectedKey1, expectedJson1);
        String expectedKey2 = "data";
        String expectedJson2 = "{\"key\":\"value\"}";
        map.put(expectedKey2, expectedJson2);

        ArrayList<ArrayList<String>> result = udf.evaluate(map);

        Assertions.assertEquals(2, result.size());
        String actualJson1 = null;
        String actualJson2 = null;
        for (ArrayList<String> entry : result) {
            String key = entry.get(0);
            if ("ext".equals(key)) {
                actualJson1 = entry.get(1);
            } else if ("data".equals(key)) {
                actualJson2 = entry.get(1);
            }
            if (actualJson1 != null && actualJson2 != null) {
                break;
            }
        }

        Assertions.assertEquals(
            normalizeJson(expectedJson1),
            normalizeJson(actualJson1)
        );
        Assertions.assertEquals(
            normalizeJson(expectedJson2),
            normalizeJson(actualJson2)
        );
    }

    private String normalizeJson(String json) {
        if (json == null) return null;
        return json.replaceAll("\\s+", ""); // 移除所有空白字符
    }

    public static Map.Entry<String, Object> createEntry(String key, Object value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

}
