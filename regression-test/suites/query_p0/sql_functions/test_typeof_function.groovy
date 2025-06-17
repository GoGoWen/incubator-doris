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

suite("test_typeof_function") {
    sql 'set enable_nereids_planner=false'
    sql 'set enable_fallback_to_original_planner=true'
    
    // Test basic types with legacy planner
    qt_sql_typeof_int "select typeof(1)"
    qt_sql_typeof_bigint "select typeof(cast(1 as bigint))"
    qt_sql_typeof_smallint "select typeof(cast(1 as smallint))"
    qt_sql_typeof_tinyint "select typeof(cast(1 as tinyint))"
    qt_sql_typeof_largeint "select typeof(cast(1 as largeint))"
    qt_sql_typeof_float "select typeof(cast(1.0 as float))"
    qt_sql_typeof_double "select typeof(1.0)"
    qt_sql_typeof_varchar "select typeof('hello')"
    qt_sql_typeof_string "select typeof(cast('hello' as string))"
    qt_sql_typeof_boolean "select typeof(true)"
    qt_sql_typeof_null "select typeof(null)"
    
    // Test decimal types
    qt_sql_typeof_decimal "select typeof(cast(123.45 as decimal(10,2)))"
    qt_sql_typeof_decimalv2 "select typeof(cast(123.45 as decimalv2(10,2)))"
    
    // Test date/time types
    qt_sql_typeof_date "select typeof(cast('2023-01-01' as date))"
    qt_sql_typeof_datetime "select typeof(cast('2023-01-01 12:00:00' as datetime))"
    qt_sql_typeof_datev2 "select typeof(cast('2023-01-01' as datev2))"
    qt_sql_typeof_datetimev2 "select typeof(cast('2023-01-01 12:00:00' as datetimev2))"
    
    // Test expressions
    qt_sql_typeof_expression "select typeof(1 + 2)"
    qt_sql_typeof_function "select typeof(abs(-5))"
    qt_sql_typeof_concat "select typeof(concat('hello', 'world'))"
    
    // Test array types
    qt_sql_typeof_array_int "select typeof(array(1, 2, 3))"
    qt_sql_typeof_array_string "select typeof(array('a', 'b', 'c'))"
    
    // Test nullable behavior
    qt_sql_typeof_nullable "select typeof(cast(null as int))"
    
    // Test with arithmetic operations
    qt_sql_typeof_arithmetic "select typeof(1 + 2.5)"
    qt_sql_typeof_division "select typeof(10 / 3)"
    
    // Test with function calls
    qt_sql_typeof_upper "select typeof(upper('test'))"
    qt_sql_typeof_length "select typeof(length('test'))"
    qt_sql_typeof_round "select typeof(round(3.14159, 2))"
    
    // Test with conditional expressions
    qt_sql_typeof_case "select typeof(case when 1=1 then 'true' else 'false' end)"
    qt_sql_typeof_if "select typeof(if(true, 1, 0))"
    
    // Test with casting
    qt_sql_typeof_cast_to_string "select typeof(cast(123 as string))"
    qt_sql_typeof_cast_to_int "select typeof(cast('123' as int))"
    
    // Test edge cases
    qt_sql_typeof_zero "select typeof(0)"
    qt_sql_typeof_negative "select typeof(-1)"
    qt_sql_typeof_large_number "select typeof(9223372036854775807)"
    qt_sql_typeof_unicode "select typeof('测试中文')"
}
