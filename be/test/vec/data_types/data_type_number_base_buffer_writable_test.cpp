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

#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class DataTypeNumberBaseBufferWritableTest : public testing::Test {
public:
    DataTypeNumberBaseBufferWritableTest() = default;
    virtual ~DataTypeNumberBaseBufferWritableTest() = default;

protected:
    // Helper function to test BufferWritable to_string method
    template <typename T>
    std::string test_buffer_writable_to_string(const DataTypeNumberBase<T>& data_type, T value) {
        auto column = ColumnVector<T>::create();
        column->insert_value(value);
        
        auto string_column = ColumnString::create();
        BufferWritable buffer(*string_column);
        
        data_type.to_string(*column, 0, buffer);
        buffer.commit();
        
        return string_column->get_data_at(0).to_string();
    }
};

// Test Float32 (float) BufferWritable formatting with .0 appending
TEST_F(DataTypeNumberBaseBufferWritableTest, Float32BufferWritableBasic) {
    DataTypeFloat32 data_type;
    
    // Test integer-valued floats should have .0 appended
    EXPECT_EQ("1.0", test_buffer_writable_to_string(data_type, 1.0f));
    EXPECT_EQ("42.0", test_buffer_writable_to_string(data_type, 42.0f));
    EXPECT_EQ("0.0", test_buffer_writable_to_string(data_type, 0.0f));
    EXPECT_EQ("-1.0", test_buffer_writable_to_string(data_type, -1.0f));
    EXPECT_EQ("-42.0", test_buffer_writable_to_string(data_type, -42.0f));
    EXPECT_EQ("9.0", test_buffer_writable_to_string(data_type, 9.0f));
    
    // Test floats that already have decimal points should remain unchanged
    EXPECT_EQ("1.5", test_buffer_writable_to_string(data_type, 1.5f));
    EXPECT_EQ("3.14159", test_buffer_writable_to_string(data_type, 3.14159f));
    EXPECT_EQ("-2.5", test_buffer_writable_to_string(data_type, -2.5f));
    EXPECT_EQ("0.1", test_buffer_writable_to_string(data_type, 0.1f));
}

TEST_F(DataTypeNumberBaseBufferWritableTest, Float32BufferWritableSpecialValues) {
    DataTypeFloat32 data_type;

    // Test NaN - should be formatted as "NaN"
    float nan_val = std::numeric_limits<float>::quiet_NaN();
    std::string nan_result = test_buffer_writable_to_string(data_type, nan_val);
    EXPECT_EQ("NaN", nan_result);

    // Test positive infinity - should be formatted as "Infinity"
    float inf_val = std::numeric_limits<float>::infinity();
    std::string inf_result = test_buffer_writable_to_string(data_type, inf_val);
    EXPECT_EQ("Infinity", inf_result);

    // Test negative infinity - should be formatted as "-Infinity"
    float neg_inf_val = -std::numeric_limits<float>::infinity();
    std::string neg_inf_result = test_buffer_writable_to_string(data_type, neg_inf_val);
    EXPECT_EQ("-Infinity", neg_inf_result);
}

TEST_F(DataTypeNumberBaseBufferWritableTest, Float32BufferWritableScientificNotation) {
    DataTypeFloat32 data_type;

    // Test very large numbers (should be in scientific notation)
    float max_val = std::numeric_limits<float>::max();
    std::string max_result = test_buffer_writable_to_string(data_type, max_val);
    // Very large numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(max_result.find("e") != std::string::npos ||
                max_result.find("E") != std::string::npos);
    EXPECT_NE(max_result.substr(max_result.length() - 2), ".0");

    // Test very small numbers (should be in scientific notation)
    float min_val = std::numeric_limits<float>::min();
    std::string min_result = test_buffer_writable_to_string(data_type, min_val);
    // Very small numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(min_result.find("e") != std::string::npos ||
                min_result.find("E") != std::string::npos);
    EXPECT_NE(min_result.substr(min_result.length() - 2), ".0");

    // Test explicit scientific notation values
    float sci_val = 1e10f;
    std::string sci_result = test_buffer_writable_to_string(data_type, sci_val);
    // Should either be in scientific notation or a large integer with .0
    if (sci_result.find("e") != std::string::npos ||
        sci_result.find("E") != std::string::npos) {
        // If in scientific notation, should not end with ".0"
        EXPECT_NE(sci_result.substr(sci_result.length() - 2), ".0");
    } else {
        // If not in scientific notation, should end with ".0" for integer values
        EXPECT_EQ(sci_result.substr(sci_result.length() - 2), ".0");
    }
}

TEST_F(DataTypeNumberBaseBufferWritableTest, Float32BufferWritableNegativeZero) {
    DataTypeFloat32 data_type;
    
    // Test negative zero
    float neg_zero = -0.0f;
    std::string neg_zero_result = test_buffer_writable_to_string(data_type, neg_zero);
    // Should have .0 appended (whether it shows as -0.0 or 0.0 depends on implementation)
    EXPECT_TRUE(neg_zero_result.find(".0") != std::string::npos);
}

// Test Float64 (double) BufferWritable formatting with .0 appending
TEST_F(DataTypeNumberBaseBufferWritableTest, Float64BufferWritableBasic) {
    DataTypeFloat64 data_type;
    
    // Test integer-valued doubles should have .0 appended
    EXPECT_EQ("1.0", test_buffer_writable_to_string(data_type, 1.0));
    EXPECT_EQ("42.0", test_buffer_writable_to_string(data_type, 42.0));
    EXPECT_EQ("0.0", test_buffer_writable_to_string(data_type, 0.0));
    EXPECT_EQ("-1.0", test_buffer_writable_to_string(data_type, -1.0));
    EXPECT_EQ("-42.0", test_buffer_writable_to_string(data_type, -42.0));
    EXPECT_EQ("9.0", test_buffer_writable_to_string(data_type, 9.0));
    
    // Test doubles that already have decimal points should remain unchanged
    EXPECT_EQ("1.5", test_buffer_writable_to_string(data_type, 1.5));
    EXPECT_EQ("3.14159265359", test_buffer_writable_to_string(data_type, 3.14159265359));
    EXPECT_EQ("-2.5", test_buffer_writable_to_string(data_type, -2.5));
    EXPECT_EQ("0.1", test_buffer_writable_to_string(data_type, 0.1));
}

TEST_F(DataTypeNumberBaseBufferWritableTest, Float64BufferWritableSpecialValues) {
    DataTypeFloat64 data_type;
    
    // Test NaN - should be formatted as "NaN"
    double nan_val = std::numeric_limits<double>::quiet_NaN();
    std::string nan_result = test_buffer_writable_to_string(data_type, nan_val);
    EXPECT_EQ("NaN", nan_result);

    // Test positive infinity - should be formatted as "Infinity"
    double inf_val = std::numeric_limits<double>::infinity();
    std::string inf_result = test_buffer_writable_to_string(data_type, inf_val);
    EXPECT_EQ("Infinity", inf_result);

    // Test negative infinity - should be formatted as "-Infinity"
    double neg_inf_val = -std::numeric_limits<double>::infinity();
    std::string neg_inf_result = test_buffer_writable_to_string(data_type, neg_inf_val);
    EXPECT_EQ("-Infinity", neg_inf_result);
}

TEST_F(DataTypeNumberBaseBufferWritableTest, Float64BufferWritableScientificNotation) {
    DataTypeFloat64 data_type;

    // Test very large numbers (should be in scientific notation)
    double max_val = std::numeric_limits<double>::max();
    std::string max_result = test_buffer_writable_to_string(data_type, max_val);
    // Very large numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(max_result.find("e") != std::string::npos ||
                max_result.find("E") != std::string::npos);
    EXPECT_NE(max_result.substr(max_result.length() - 2), ".0");

    // Test very small numbers (should be in scientific notation)
    double min_val = std::numeric_limits<double>::min();
    std::string min_result = test_buffer_writable_to_string(data_type, min_val);
    // Very small numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(min_result.find("e") != std::string::npos ||
                min_result.find("E") != std::string::npos);
    EXPECT_NE(min_result.substr(min_result.length() - 2), ".0");
}

// Test integral types should not be affected by the changes
TEST_F(DataTypeNumberBaseBufferWritableTest, IntegralTypesBufferWritable) {
    DataTypeInt32 int_data_type;
    DataTypeInt64 long_data_type;

    // Test that integral types are not affected by floating-point changes
    EXPECT_EQ("42", test_buffer_writable_to_string(int_data_type, 42));
    EXPECT_EQ("-42", test_buffer_writable_to_string(int_data_type, -42));
    EXPECT_EQ("0", test_buffer_writable_to_string(int_data_type, 0));

    EXPECT_EQ("123456789", test_buffer_writable_to_string(long_data_type, 123456789L));
    EXPECT_EQ("-123456789", test_buffer_writable_to_string(long_data_type, -123456789L));
    EXPECT_EQ("0", test_buffer_writable_to_string(long_data_type, 0L));
}

// Test edge cases for float precision and boundary values
TEST_F(DataTypeNumberBaseBufferWritableTest, Float32BufferWritableEdgeCases) {
    DataTypeFloat32 data_type;

    // Test boundary values that should get .0 appended
    EXPECT_EQ("1.0", test_buffer_writable_to_string(data_type, 1.0f));
    EXPECT_EQ("2.0", test_buffer_writable_to_string(data_type, 2.0f));
    EXPECT_EQ("10.0", test_buffer_writable_to_string(data_type, 10.0f));
    EXPECT_EQ("100.0", test_buffer_writable_to_string(data_type, 100.0f));
    EXPECT_EQ("1000.0", test_buffer_writable_to_string(data_type, 1000.0f));

    // Test negative boundary values
    EXPECT_EQ("-1.0", test_buffer_writable_to_string(data_type, -1.0f));
    EXPECT_EQ("-10.0", test_buffer_writable_to_string(data_type, -10.0f));
    EXPECT_EQ("-100.0", test_buffer_writable_to_string(data_type, -100.0f));

    // Test values that should NOT get .0 appended (already have decimals)
    EXPECT_EQ("1.1", test_buffer_writable_to_string(data_type, 1.1f));
    EXPECT_EQ("10.01", test_buffer_writable_to_string(data_type, 10.01f));
    EXPECT_EQ("100.001", test_buffer_writable_to_string(data_type, 100.001f));
}

// Test edge cases for double precision and boundary values
TEST_F(DataTypeNumberBaseBufferWritableTest, Float64BufferWritableEdgeCases) {
    DataTypeFloat64 data_type;

    // Test boundary values that should get .0 appended
    EXPECT_EQ("1.0", test_buffer_writable_to_string(data_type, 1.0));
    EXPECT_EQ("2.0", test_buffer_writable_to_string(data_type, 2.0));
    EXPECT_EQ("10.0", test_buffer_writable_to_string(data_type, 10.0));
    EXPECT_EQ("100.0", test_buffer_writable_to_string(data_type, 100.0));
    EXPECT_EQ("1000.0", test_buffer_writable_to_string(data_type, 1000.0));

    // Test negative boundary values
    EXPECT_EQ("-1.0", test_buffer_writable_to_string(data_type, -1.0));
    EXPECT_EQ("-10.0", test_buffer_writable_to_string(data_type, -10.0));
    EXPECT_EQ("-100.0", test_buffer_writable_to_string(data_type, -100.0));

    // Test values that should NOT get .0 appended (already have decimals)
    EXPECT_EQ("1.1", test_buffer_writable_to_string(data_type, 1.1));
    EXPECT_EQ("10.01", test_buffer_writable_to_string(data_type, 10.01));
    EXPECT_EQ("100.001", test_buffer_writable_to_string(data_type, 100.001));

    // Test high precision values
    EXPECT_EQ("3.141592653589793", test_buffer_writable_to_string(data_type, 3.141592653589793));
}

// Test multiple values in the same column
TEST_F(DataTypeNumberBaseBufferWritableTest, MultipleValuesBufferWritable) {
    DataTypeFloat32 float_data_type;
    DataTypeFloat64 double_data_type;

    // Test multiple float values
    auto float_column = ColumnFloat32::create();
    float_column->insert_value(1.0f);
    float_column->insert_value(2.5f);
    float_column->insert_value(std::numeric_limits<float>::quiet_NaN());
    float_column->insert_value(std::numeric_limits<float>::infinity());
    float_column->insert_value(-std::numeric_limits<float>::infinity());
    float_column->insert_value(42.0f);

    auto string_column = ColumnString::create();
    BufferWritable buffer(*string_column);

    for (size_t i = 0; i < float_column->size(); ++i) {
        float_data_type.to_string(*float_column, i, buffer);
        buffer.commit();
    }

    EXPECT_EQ("1.0", string_column->get_data_at(0).to_string());
    EXPECT_EQ("2.5", string_column->get_data_at(1).to_string());
    EXPECT_EQ("NaN", string_column->get_data_at(2).to_string());
    EXPECT_EQ("Infinity", string_column->get_data_at(3).to_string());
    EXPECT_EQ("-Infinity", string_column->get_data_at(4).to_string());
    EXPECT_EQ("42.0", string_column->get_data_at(5).to_string());
}

// Test consistency between different to_string methods
TEST_F(DataTypeNumberBaseBufferWritableTest, ConsistencyBetweenToStringMethods) {
    DataTypeFloat32 float_data_type;
    DataTypeFloat64 double_data_type;

    // Test values that should produce consistent results across all to_string methods
    std::vector<float> float_test_values = {
        1.0f, 42.0f, -1.0f, 0.0f, 1.5f, -2.5f,
        std::numeric_limits<float>::quiet_NaN(),
        std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity()
    };

    std::vector<double> double_test_values = {
        1.0, 42.0, -1.0, 0.0, 1.5, -2.5,
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()
    };

    // Test float consistency
    for (float value : float_test_values) {
        auto column = ColumnFloat32::create();
        column->insert_value(value);

        // Method 1: to_string(const T& value)
        std::string method1_result = float_data_type.to_string(value);

        // Method 2: to_string(const IColumn& column, size_t row_num)
        std::string method2_result = float_data_type.to_string(*column, 0);

        // Method 3: to_string(const IColumn& column, size_t row_num, BufferWritable& ostr)
        std::string method3_result = test_buffer_writable_to_string(float_data_type, value);

        // All methods should produce the same result
        EXPECT_EQ(method1_result, method2_result) << "Float value: " << value;
        EXPECT_EQ(method1_result, method3_result) << "Float value: " << value;
        EXPECT_EQ(method2_result, method3_result) << "Float value: " << value;
    }

    // Test double consistency
    for (double value : double_test_values) {
        auto column = ColumnFloat64::create();
        column->insert_value(value);

        // Method 1: to_string(const T& value)
        std::string method1_result = double_data_type.to_string(value);

        // Method 2: to_string(const IColumn& column, size_t row_num)
        std::string method2_result = double_data_type.to_string(*column, 0);

        // Method 3: to_string(const IColumn& column, size_t row_num, BufferWritable& ostr)
        std::string method3_result = test_buffer_writable_to_string(double_data_type, value);

        // All methods should produce the same result
        EXPECT_EQ(method1_result, method2_result) << "Double value: " << value;
        EXPECT_EQ(method1_result, method3_result) << "Double value: " << value;
        EXPECT_EQ(method2_result, method3_result) << "Double value: " << value;
    }
}

} // namespace doris::vectorized
