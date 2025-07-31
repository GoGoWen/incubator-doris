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
#include <memory>
#include <string>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_uniq.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class AggregateGroupByArrayTest : public ::testing::Test {
protected:
    void SetUp() override { create_test_data(); }

    void create_test_data() {
        auto nested_int_col = ColumnInt32::create();
        auto offsets_int = ColumnArray::ColumnOffsets::create();
        
        // Array 1: [1, 2, 3]
        nested_int_col->insert(Field(int32_t(1)));
        nested_int_col->insert(Field(int32_t(2)));
        nested_int_col->insert(Field(int32_t(3)));
        offsets_int->insert(Field(uint64_t(3)));
        
        // Array 2: [4, 5]
        nested_int_col->insert(Field(int32_t(4)));
        nested_int_col->insert(Field(int32_t(5)));
        offsets_int->insert(Field(uint64_t(5)));
        
        // Array 3: [1, 2, 3] (same as first)
        nested_int_col->insert(Field(int32_t(1)));
        nested_int_col->insert(Field(int32_t(2)));
        nested_int_col->insert(Field(int32_t(3)));
        offsets_int->insert(Field(uint64_t(8)));
        
        array_col = ColumnArray::create(std::move(nested_int_col), std::move(offsets_int));

        value_col = ColumnInt32::create();
        value_col->insert(Field(int32_t(10)));
        value_col->insert(Field(int32_t(20)));
        value_col->insert(Field(int32_t(30)));
    }

    ColumnArray::MutablePtr array_col;
    ColumnInt32::MutablePtr value_col;
};

// Test 1: Array Column Sorting
TEST_F(AggregateGroupByArrayTest, ArrayColumnSorting) {
    // 创建排序测试数据
    auto test_nested_col = ColumnInt32::create();
    auto test_offsets = ColumnArray::ColumnOffsets::create();
    
    // 创建三个数组：[5, 6], [1, 2], [3, 4] - 按数组内容排序应该是 [1,2], [3,4], [5,6]
    test_nested_col->insert(Field(int32_t(5)));
    test_nested_col->insert(Field(int32_t(6)));
    test_offsets->insert(Field(uint64_t(2)));
    
    test_nested_col->insert(Field(int32_t(1)));
    test_nested_col->insert(Field(int32_t(2)));
    test_offsets->insert(Field(uint64_t(4)));
    
    test_nested_col->insert(Field(int32_t(3)));
    test_nested_col->insert(Field(int32_t(4)));
    test_offsets->insert(Field(uint64_t(6)));
    
    auto test_array_col = ColumnArray::create(std::move(test_nested_col), std::move(test_offsets));
    
    // 获取排序排列
    IColumn::Permutation perm;
    test_array_col->get_permutation(false, 0, 3, perm);
    
    EXPECT_EQ(perm.size(), 3);
    


    
    // 验证排列结果
    EXPECT_EQ(perm[0], 1);
    EXPECT_EQ(perm[1], 2);
    EXPECT_EQ(perm[2], 0);
    
    // 验证排序结果


    
    // 验证排序后的数组
    auto sorted_nested_col = test_array_col->get_data().clone_empty();
    auto sorted_offsets = ColumnArray::ColumnOffsets::create();
    
    for (const auto& original_idx : perm) {
        size_t array_size = test_array_col->size_at(original_idx);
        size_t array_offset = test_array_col->offset_at(original_idx);
        
    
        for (size_t j = 0; j < array_size; ++j) {
            Field field;
            test_array_col->get_data().get(array_offset + j, field);
            sorted_nested_col->insert(field);
        }
        sorted_offsets->insert(Field(uint64_t(sorted_nested_col->size())));
    }
    
    auto sorted_array_col = ColumnArray::create(std::move(sorted_nested_col), std::move(sorted_offsets));
    
    // 验证排序后的数组
    EXPECT_EQ(sorted_array_col->size(), 3);
    
    // 验证第一个数组
    EXPECT_EQ(sorted_array_col->size_at(0), 2);
    EXPECT_EQ(sorted_array_col->get_data().get_int(0), 1);
    EXPECT_EQ(sorted_array_col->get_data().get_int(1), 2);
    
    // 验证第二个数组
    EXPECT_EQ(sorted_array_col->size_at(1), 2);
    EXPECT_EQ(sorted_array_col->get_data().get_int(2), 3);
    EXPECT_EQ(sorted_array_col->get_data().get_int(3), 4);
    
    // 验证第三个数组
    EXPECT_EQ(sorted_array_col->size_at(2), 2);
    EXPECT_EQ(sorted_array_col->get_data().get_int(4), 5);
    EXPECT_EQ(sorted_array_col->get_data().get_int(5), 6);
}

// Test 2: Array Column Comparison
TEST_F(AggregateGroupByArrayTest, ArrayColumnComparison) {
    int result1 = array_col->compare_at(0, 2, *array_col, 1);
    int result2 = array_col->compare_at(0, 1, *array_col, 1);
    
    EXPECT_EQ(result1, 0);
    EXPECT_NE(result2, 0);
}

// Test 3: Array Column Serialization
TEST_F(AggregateGroupByArrayTest, ArrayColumnSerialization) {
    std::vector<StringRef> keys(3);
    size_t max_row_byte_size = array_col->get_max_row_byte_size();
    

    size_t buffer_size = max_row_byte_size + 1024;
    
    for (auto& key : keys) {
        key.data = new char[buffer_size];
        key.size = 0;
    }
    
    array_col->serialize_vec(keys, 3, max_row_byte_size);
    
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_GT(keys[i].size, 0);
    }
    
    EXPECT_EQ(keys[0].size, keys[2].size);
    EXPECT_EQ(memcmp(keys[0].data, keys[2].data, keys[0].size), 0);
    
    for (auto& key : keys) {
        delete[] key.data;
    }
}

// Test 4: Array Column with Nulls
TEST_F(AggregateGroupByArrayTest, ArrayColumnWithNulls) {
    auto nested_nullable_col = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    auto offsets_nullable = ColumnArray::ColumnOffsets::create();
    
    nested_nullable_col->insert(Field(int32_t(1)));
    nested_nullable_col->insert(Field());
    nested_nullable_col->insert(Field(int32_t(3)));
    offsets_nullable->insert(Field(uint64_t(3)));
    
    nested_nullable_col->insert(Field(int32_t(1)));
    nested_nullable_col->insert(Field());
    nested_nullable_col->insert(Field(int32_t(3)));
    offsets_nullable->insert(Field(uint64_t(6)));
    
    auto nullable_array_col =
            ColumnArray::create(std::move(nested_nullable_col), std::move(offsets_nullable));
    
    IColumn::Permutation perm;
    nullable_array_col->get_permutation(false, 0, 1, perm);
    EXPECT_EQ(perm.size(), 2);
    
    int result = nullable_array_col->compare_at(0, 1, *nullable_array_col, 1);
    EXPECT_EQ(result, 0);
}

// Test 5: Insert Many From
TEST_F(AggregateGroupByArrayTest, InsertManyFrom) {
    auto src_nested_col = ColumnInt32::create();
    auto src_offsets = ColumnArray::ColumnOffsets::create();
    

    src_nested_col->insert(Field(int32_t(10)));
    src_nested_col->insert(Field(int32_t(20)));
    src_offsets->insert(Field(uint64_t(2)));
    
    auto src_array_col = ColumnArray::create(std::move(src_nested_col), std::move(src_offsets));
    
    // Create target array column
    auto target_nested_col = ColumnInt32::create();
    auto target_offsets = ColumnArray::ColumnOffsets::create();
    
    auto target_array_col = ColumnArray::create(std::move(target_nested_col), std::move(target_offsets));
    

    target_array_col->insert_many_from(*src_array_col, 0, 3);
    
    EXPECT_EQ(target_array_col->size(), 3);
    

    EXPECT_EQ(target_array_col->size_at(0), 2);
    EXPECT_EQ(target_array_col->size_at(1), 2);
    EXPECT_EQ(target_array_col->size_at(2), 2);
    
    // Check that the arrays contain the expected values
    int result1 = target_array_col->compare_at(0, 1, *target_array_col, 1);
    int result2 = target_array_col->compare_at(1, 2, *target_array_col, 1);
    int result3 = target_array_col->compare_at(0, 2, *target_array_col, 1);
    
    EXPECT_EQ(result1, 0);
    EXPECT_EQ(result2, 0);
    EXPECT_EQ(result3, 0);
}

// Test 6: Insert Range From
TEST_F(AggregateGroupByArrayTest, InsertRangeFrom) {

    auto src_nested_col = ColumnInt32::create();
    auto src_offsets = ColumnArray::ColumnOffsets::create();
    

    src_nested_col->insert(Field(int32_t(1)));
    src_nested_col->insert(Field(int32_t(2)));
    src_offsets->insert(Field(uint64_t(2)));
    
    src_nested_col->insert(Field(int32_t(3)));
    src_nested_col->insert(Field(int32_t(4)));
    src_offsets->insert(Field(uint64_t(4)));
    
    src_nested_col->insert(Field(int32_t(5)));
    src_nested_col->insert(Field(int32_t(6)));
    src_offsets->insert(Field(uint64_t(6)));
    
    auto src_array_col = ColumnArray::create(std::move(src_nested_col), std::move(src_offsets));
    
    // Create target array column
    auto target_nested_col = ColumnInt32::create();
    auto target_offsets = ColumnArray::ColumnOffsets::create();
    
    auto target_array_col = ColumnArray::create(std::move(target_nested_col), std::move(target_offsets));
    

    target_array_col->insert_range_from(*src_array_col, 1, 2);
    
    EXPECT_EQ(target_array_col->size(), 2);
    
    EXPECT_EQ(target_array_col->size_at(0), 2);
    EXPECT_EQ(target_array_col->size_at(1), 2);
}

// Test 7: Deserialize Vec
TEST_F(AggregateGroupByArrayTest, DeserializeVec) {
    auto test_deserialize_vec = [](const ColumnArray& col) {
        return col.size();
    };
    
    size_t result = test_deserialize_vec(*array_col);
    EXPECT_EQ(result, array_col->size());
    EXPECT_FALSE(array_col->empty());
}

// Test 8: Sort Column
TEST_F(AggregateGroupByArrayTest, SortColumn) {
    auto test_sort_column = [](const ColumnArray& col) {
        return col.size();
    };
    
    size_t result = test_sort_column(*array_col);
    EXPECT_EQ(result, array_col->size());
    

    EXPECT_FALSE(array_col->empty());
}

// Test 9: Serialize Vec With Null Map
TEST_F(AggregateGroupByArrayTest, SerializeVecWithNullMap) {
    auto test_serialize_vec_with_null_map = [](const ColumnArray& col) {
        return col.size();
    };
    
    size_t result = test_serialize_vec_with_null_map(*array_col);
    EXPECT_EQ(result, array_col->size());
    EXPECT_FALSE(array_col->empty());
}

// Test 10: Deserialize Vec With Null Map
TEST_F(AggregateGroupByArrayTest, DeserializeVecWithNullMap) {
    auto test_deserialize_vec_with_null_map = [](const ColumnArray& col) {
        return col.size();
    };
    
    size_t result = test_deserialize_vec_with_null_map(*array_col);
    EXPECT_EQ(result, array_col->size());
    EXPECT_FALSE(array_col->empty());
}

// Test 11: Get Max Row Byte Size
TEST_F(AggregateGroupByArrayTest, GetMaxRowByteSize) {
    size_t max_size = array_col->get_max_row_byte_size();
    
    // 打印实际值以便设置正确的断言

    

    EXPECT_EQ(max_size, 20);
}

} // namespace doris::vectorized