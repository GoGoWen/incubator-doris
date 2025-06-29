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

#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class DataTypeNumberBaseFloatTest : public testing::Test {
public:
    DataTypeNumberBaseFloatTest() = default;
    virtual ~DataTypeNumberBaseFloatTest() = default;
};

// Test Float32 (float) formatting
TEST_F(DataTypeNumberBaseFloatTest, Float32ToStringBasic) {
    DataTypeFloat32 data_type;
    
    // Test integer-valued floats should have .0 appended
    EXPECT_EQ("1.0", data_type.to_string(1.0f));
    EXPECT_EQ("42.0", data_type.to_string(42.0f));
    EXPECT_EQ("0.0", data_type.to_string(0.0f));
    EXPECT_EQ("-1.0", data_type.to_string(-1.0f));
    EXPECT_EQ("-42.0", data_type.to_string(-42.0f));
    
    // Test floats that already have decimal points should remain unchanged
    EXPECT_EQ("1.5", data_type.to_string(1.5f));
    EXPECT_EQ("3.14159", data_type.to_string(3.14159f));
    EXPECT_EQ("-2.5", data_type.to_string(-2.5f));
    EXPECT_EQ("0.1", data_type.to_string(0.1f));
}

TEST_F(DataTypeNumberBaseFloatTest, Float32ToStringSpecialValues) {
    DataTypeFloat32 data_type;

    // Test NaN - should be formatted as "NaN"
    float nan_val = std::numeric_limits<float>::quiet_NaN();
    std::string nan_result = data_type.to_string(nan_val);
    EXPECT_EQ("NaN", nan_result);

    // Test positive infinity - should be formatted as "Infinity"
    float inf_val = std::numeric_limits<float>::infinity();
    std::string inf_result = data_type.to_string(inf_val);
    EXPECT_EQ("Infinity", inf_result);

    // Test negative infinity - should be formatted as "-Infinity"
    float neg_inf_val = -std::numeric_limits<float>::infinity();
    std::string neg_inf_result = data_type.to_string(neg_inf_val);
    EXPECT_EQ("-Infinity", neg_inf_result);
}

TEST_F(DataTypeNumberBaseFloatTest, Float32ToStringScientificNotation) {
    DataTypeFloat32 data_type;

    // Test very large numbers (should be in scientific notation)
    float max_val = std::numeric_limits<float>::max();
    std::string max_result = data_type.to_string(max_val);
    // Very large numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(max_result.find("e") != std::string::npos ||
                max_result.find("E") != std::string::npos);
    EXPECT_NE(max_result.substr(max_result.length() - 2), ".0");

    // Test very small numbers (should be in scientific notation)
    float min_val = std::numeric_limits<float>::min();
    std::string min_result = data_type.to_string(min_val);
    // Very small numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(min_result.find("e") != std::string::npos ||
                min_result.find("E") != std::string::npos);
    EXPECT_NE(min_result.substr(min_result.length() - 2), ".0");

    // Test explicit scientific notation values
    float sci_val = 1e10f;
    std::string sci_result = data_type.to_string(sci_val);
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

TEST_F(DataTypeNumberBaseFloatTest, Float32ToStringNegativeZero) {
    DataTypeFloat32 data_type;
    
    // Test negative zero
    float neg_zero = -0.0f;
    std::string neg_zero_result = data_type.to_string(neg_zero);
    // Should have .0 appended (whether it shows as -0.0 or 0.0 depends on implementation)
    EXPECT_TRUE(neg_zero_result.find(".0") != std::string::npos);
}

// Test Float64 (double) formatting
TEST_F(DataTypeNumberBaseFloatTest, Float64ToStringBasic) {
    DataTypeFloat64 data_type;
    
    // Test integer-valued doubles should have .0 appended
    EXPECT_EQ("1.0", data_type.to_string(1.0));
    EXPECT_EQ("42.0", data_type.to_string(42.0));
    EXPECT_EQ("0.0", data_type.to_string(0.0));
    EXPECT_EQ("-1.0", data_type.to_string(-1.0));
    EXPECT_EQ("-42.0", data_type.to_string(-42.0));
    
    // Test doubles that already have decimal points should remain unchanged
    EXPECT_EQ("1.5", data_type.to_string(1.5));
    EXPECT_EQ("3.14159265359", data_type.to_string(3.14159265359));
    EXPECT_EQ("-2.5", data_type.to_string(-2.5));
    EXPECT_EQ("0.1", data_type.to_string(0.1));
}

TEST_F(DataTypeNumberBaseFloatTest, Float64ToStringSpecialValues) {
    DataTypeFloat64 data_type;
    
    // Test NaN - should be formatted as "NaN"
    double nan_val = std::numeric_limits<double>::quiet_NaN();
    std::string nan_result = data_type.to_string(nan_val);
    EXPECT_EQ("NaN", nan_result);

    // Test positive infinity - should be formatted as "Infinity"
    double inf_val = std::numeric_limits<double>::infinity();
    std::string inf_result = data_type.to_string(inf_val);
    EXPECT_EQ("Infinity", inf_result);

    // Test negative infinity - should be formatted as "-Infinity"
    double neg_inf_val = -std::numeric_limits<double>::infinity();
    std::string neg_inf_result = data_type.to_string(neg_inf_val);
    EXPECT_EQ("-Infinity", neg_inf_result);
}

TEST_F(DataTypeNumberBaseFloatTest, Float64ToStringScientificNotation) {
    DataTypeFloat64 data_type;

    // Test very large numbers (should be in scientific notation)
    double max_val = std::numeric_limits<double>::max();
    std::string max_result = data_type.to_string(max_val);
    // Very large numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(max_result.find("e") != std::string::npos ||
                max_result.find("E") != std::string::npos);
    EXPECT_NE(max_result.substr(max_result.length() - 2), ".0");

    // Test very small numbers (should be in scientific notation)
    double min_val = std::numeric_limits<double>::min();
    std::string min_result = data_type.to_string(min_val);
    // Very small numbers should be in scientific notation and not end with ".0"
    EXPECT_TRUE(min_result.find("e") != std::string::npos ||
                min_result.find("E") != std::string::npos);
    EXPECT_NE(min_result.substr(min_result.length() - 2), ".0");
}

// Test column-based to_string method
TEST_F(DataTypeNumberBaseFloatTest, Float32ColumnToString) {
    DataTypeFloat32 data_type;
    auto column = ColumnFloat32::create();
    
    // Add test values
    column->insert_value(1.0f);
    column->insert_value(2.5f);
    column->insert_value(std::numeric_limits<float>::quiet_NaN());
    column->insert_value(std::numeric_limits<float>::infinity());
    
    // Test column-based to_string
    EXPECT_EQ("1.0", data_type.to_string(*column, 0));
    EXPECT_EQ("2.5", data_type.to_string(*column, 1));
    
    // Test NaN and infinity
    std::string nan_result = data_type.to_string(*column, 2);
    EXPECT_EQ("NaN", nan_result);

    std::string inf_result = data_type.to_string(*column, 3);
    EXPECT_EQ("Infinity", inf_result);
}

TEST_F(DataTypeNumberBaseFloatTest, Float64ColumnToString) {
    DataTypeFloat64 data_type;
    auto column = ColumnFloat64::create();
    
    // Add test values
    column->insert_value(1.0);
    column->insert_value(2.5);
    column->insert_value(std::numeric_limits<double>::quiet_NaN());
    column->insert_value(std::numeric_limits<double>::infinity());
    
    // Test column-based to_string
    EXPECT_EQ("1.0", data_type.to_string(*column, 0));
    EXPECT_EQ("2.5", data_type.to_string(*column, 1));
    
    // Test NaN and infinity
    std::string nan_result = data_type.to_string(*column, 2);
    EXPECT_EQ("NaN", nan_result);

    std::string inf_result = data_type.to_string(*column, 3);
    EXPECT_EQ("Infinity", inf_result);
}

} // namespace doris::vectorized
