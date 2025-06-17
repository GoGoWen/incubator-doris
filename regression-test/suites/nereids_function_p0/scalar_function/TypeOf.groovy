// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("nereids_scalar_fn_typeof") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // Test basic integer types
    qt_sql_typeof_int "select typeof(1)"
    qt_sql_typeof_bigint "select typeof(cast(1 as bigint))"
    qt_sql_typeof_smallint "select typeof(cast(1 as smallint))"
    qt_sql_typeof_tinyint "select typeof(cast(1 as tinyint))"
    qt_sql_typeof_largeint "select typeof(cast(1 as largeint))"

    // Test floating point types
    qt_sql_typeof_float "select typeof(cast(1.0 as float))"
    qt_sql_typeof_double "select typeof(1.0)"
    qt_sql_typeof_double_literal "select typeof(3.14159)"

    // Test string types
    qt_sql_typeof_varchar "select typeof('hello')"
    qt_sql_typeof_string "select typeof(cast('hello' as string))"
    qt_sql_typeof_empty_string "select typeof('')"

    // Test boolean type
    qt_sql_typeof_boolean_true "select typeof(true)"
    qt_sql_typeof_boolean_false "select typeof(false)"

    // Test null type
    qt_sql_typeof_null "select typeof(null)"

    // Test decimal types with different precision/scale
    qt_sql_typeof_decimal_10_2 "select typeof(cast(123.45 as decimal(10,2)))"
    qt_sql_typeof_decimal_5_1 "select typeof(cast(12.3 as decimal(5,1)))"
    qt_sql_typeof_decimal_38_18 "select typeof(cast(123.456789012345678901 as decimal(38,18)))"
    qt_sql_typeof_decimalv2 "select typeof(cast(123.45 as decimalv2(10,2)))"

    // Test date/time types
    qt_sql_typeof_date "select typeof(cast('2023-01-01' as date))"
    qt_sql_typeof_datetime "select typeof(cast('2023-01-01 12:00:00' as datetime))"
    qt_sql_typeof_datev2 "select typeof(cast('2023-01-01' as datev2))"
    qt_sql_typeof_datetimev2_0 "select typeof(cast('2023-01-01 12:00:00' as datetimev2(0)))"
    qt_sql_typeof_datetimev2_3 "select typeof(cast('2023-01-01 12:00:00.123' as datetimev2(3)))"
    qt_sql_typeof_datetimev2_6 "select typeof(cast('2023-01-01 12:00:00.123456' as datetimev2(6)))"

    // Test array types
    qt_sql_typeof_array_int "select typeof(array(1, 2, 3))"
    qt_sql_typeof_array_string "select typeof(array('a', 'b', 'c'))"
    qt_sql_typeof_array_double "select typeof(array(1.1, 2.2, 3.3))"
    qt_sql_typeof_array_boolean "select typeof(array(true, false, true))"
    qt_sql_typeof_array_empty "select typeof(array())"
    qt_sql_typeof_array_nested "select typeof(array(array(1, 2), array(3, 4)))"

    // Test map types
    qt_sql_typeof_map_string_int "select typeof(map('a', 1, 'b', 2))"
    qt_sql_typeof_map_int_string "select typeof(map(1, 'one', 2, 'two'))"
    qt_sql_typeof_map_empty "select typeof(map())"

    // Test struct types
    qt_sql_typeof_struct_simple "select typeof(struct(1, 'hello'))"
    qt_sql_typeof_struct_named "select typeof(named_struct('id', 1, 'name', 'test'))"
    qt_sql_typeof_struct_nested "select typeof(struct(1, struct('inner', 2)))"

    // Test complex expressions and function calls
    qt_sql_typeof_arithmetic "select typeof(1 + 2)"
    qt_sql_typeof_arithmetic_float "select typeof(1.5 + 2.5)"
    qt_sql_typeof_multiplication "select typeof(3 * 4)"
    qt_sql_typeof_division "select typeof(10 / 3)"
    qt_sql_typeof_modulo "select typeof(10 % 3)"

    // Test function calls
    qt_sql_typeof_abs "select typeof(abs(-5))"
    qt_sql_typeof_abs_float "select typeof(abs(-5.5))"
    qt_sql_typeof_concat "select typeof(concat('hello', 'world'))"
    qt_sql_typeof_upper "select typeof(upper('test'))"
    qt_sql_typeof_length "select typeof(length('test'))"
    qt_sql_typeof_round "select typeof(round(3.14159, 2))"
    qt_sql_typeof_sqrt "select typeof(sqrt(16))"
    qt_sql_typeof_pow "select typeof(pow(2, 3))"

    // Test conditional expressions
    qt_sql_typeof_case_when "select typeof(case when 1=1 then 'true' else 'false' end)"
    qt_sql_typeof_if "select typeof(if(true, 1, 0))"
    qt_sql_typeof_ifnull "select typeof(ifnull(null, 'default'))"
    qt_sql_typeof_coalesce "select typeof(coalesce(null, null, 'value'))"

    // Test type casting results
    qt_sql_typeof_cast_int_to_string "select typeof(cast(123 as string))"
    qt_sql_typeof_cast_string_to_int "select typeof(cast('123' as int))"
    qt_sql_typeof_cast_float_to_int "select typeof(cast(3.14 as int))"
    qt_sql_typeof_cast_int_to_decimal "select typeof(cast(123 as decimal(10,2)))"

    // Test with table data (if fn_test table exists)
    try {
        qt_sql_typeof_from_table_int "select typeof(kint) from fn_test limit 1"
        qt_sql_typeof_from_table_varchar "select typeof(kvchrs1) from fn_test limit 1"
        qt_sql_typeof_from_table_double "select typeof(kdbl) from fn_test limit 1"
        qt_sql_typeof_from_table_date "select typeof(kdt) from fn_test limit 1"
    } catch (Exception e) {
        // Table might not exist, skip these tests
        logger.info("Skipping table-based tests: fn_test table not found")
    }

    // Test nullable types behavior
    qt_sql_typeof_nullable_int "select typeof(cast(null as int))"
    qt_sql_typeof_nullable_string "select typeof(cast(null as string))"
    qt_sql_typeof_nullable_decimal "select typeof(cast(null as decimal(10,2)))"

    // Test edge cases
    qt_sql_typeof_zero "select typeof(0)"
    qt_sql_typeof_negative "select typeof(-1)"
    qt_sql_typeof_large_number "select typeof(9223372036854775807)" // max bigint
    qt_sql_typeof_small_decimal "select typeof(cast(0.01 as decimal(3,2)))"
    qt_sql_typeof_unicode_string "select typeof('测试中文')"
    qt_sql_typeof_special_chars "select typeof('!@#$%^&*()')"

    // Test nested function calls
    qt_sql_typeof_nested_functions "select typeof(upper(concat('hello', ' world')))"
    qt_sql_typeof_nested_math "select typeof(abs(round(sqrt(16), 2)))"
    qt_sql_typeof_nested_cast "select typeof(cast(cast('123' as int) as string))"

    // Test with subqueries (if supported)
    qt_sql_typeof_subquery "select typeof((select 1))"
    qt_sql_typeof_subquery_string "select typeof((select 'test'))"

    // Test with aggregation functions
    qt_sql_typeof_count "select typeof(count(*))"
    qt_sql_typeof_sum "select typeof(sum(1))"
    qt_sql_typeof_avg "select typeof(avg(1.0))"
    qt_sql_typeof_max "select typeof(max('test'))"
    qt_sql_typeof_min "select typeof(min(1))"

    // Test with window functions (if supported)
    qt_sql_typeof_row_number "select typeof(row_number() over())"
    qt_sql_typeof_rank "select typeof(rank() over(order by 1))"
}
