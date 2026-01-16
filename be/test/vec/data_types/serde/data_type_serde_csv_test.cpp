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

#include "gtest/gtest_pred_impl.h"
#include "olap/types.h" // for TypeInfo
#include "olap/wrapper_field.h"
#include "vec/columns/column.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde_utils.h"
#include "vec/io/reader_buffer.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"

namespace doris::vectorized {
// This test aim to make sense for csv serde of data types.
//  we use default formatOption and special formatOption to equal serde for wrapperField.
TEST(CsvSerde, ScalaDataTypeSerdeCsvTest) {
    // arithmetic scala field types
    {
        // fieldType, test_string, expect_string
        typedef std::tuple<FieldType, std::vector<string>, std::vector<string>> FieldType_RandStr;
        std::vector<FieldType_RandStr> arithmetic_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL, {"0", "1", "-1"},
                                  {"0", "1", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_TINYINT, {"127", "-128", "-190"},
                                  {"127", "-128", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_SMALLINT, {"32767", "32768", "-32769"},
                                  {"32767", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_INT,
                                  {"2147483647", "2147483648", "-2147483649"},
                                  {"2147483647", "", ""}),
                // float ==> float32(32bit)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_FLOAT,
                                  {"1.123", "3.40282e+38", "3.40282e+38+1"},
                                  {"1.123", "3.40282e+38", ""}),
                // double ==> float64(64bit)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DOUBLE,
                                  {"2343.12345465746", "2.22507e-308", "2.22507e-308-1"},
                                  {"2343.12345465746", "2.22507e-308", ""}),
                // BIGINT ==> int64_t(64bit)
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_BIGINT,
                        {"9223372036854775807", "-9223372036854775808", "9223372036854775808"},
                        {"9223372036854775807", "-9223372036854775808", ""}),
                // LARGEINT ==> int128_t(128bit)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_LARGEINT,
                                  {"170141183460469231731687303715884105727",
                                   "−170141183460469231731687303715884105728",
                                   "170141183460469231731687303715884105728"},
                                  {"170141183460469231731687303715884105727", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_CHAR, {"amory happy"},
                                  {"amory happy"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_VARCHAR, {"doris be better"},
                                  {"doris be better"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_STRING, {"doris be better"},
                                  {"doris be better"}),
                // decimal ==> decimalv2(decimal<128>(27,9))
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {
                                // (17, 9)(first 0 will ignore)
                                "012345678901234567.012345678",
                                // (18, 8) (automatically fill 0 for scala)
                                "123456789012345678.01234567",
                                // (17, 10) (rounding last to make it fit)
                                "12345678901234567.0123456779",
                                // (17, 11) (rounding last to make it fit)
                                "12345678901234567.01234567791",
                                // (19, 8) (wrong)
                                "1234567890123456789.01234567",
                        },
                        {"12345678901234567.012345678", "123456789012345678.012345670",
                         "12345678901234567.012345678", "12345678901234567.012345678", ""}),
                // decimal32 ==>  decimal32(9,2)                       (7,2)         (6,3)         (7,3)           (8,1)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL32,
                                  {"1234567.12", "123456.123", "1234567.123", "12345679.1"},
                                  {"1234567.12", "123456.12", "1234567.12", ""}),
                // decimal64 ==> decimal64(18,9)                        (9, 9)                   (3,2)    (9, 10)                  (10, 9)
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL64,
                        {"123456789.123456789", "123.12", "123456789.0123456789",
                         "1234567890.123456789"},
                        {"123456789.123456789", "123.120000000", "123456789.012345679", ""}),
                // decimal128I ==> decimal128I(38,18)                     (19,18)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
                                  {"01234567890123456789.123456789123456789",
                                   // (20,11) (automatically fill 0 for scala)
                                   "12345678901234567890.12345678911",
                                   // (19,18)
                                   "1234567890123456789.123456789123456789",
                                   // (19,19) (rounding last to make it fit)
                                   "1234567890123456789.1234567890123456789",
                                   // (18, 20) (rounding to make it fit)
                                   "123456789012345678.01234567890123456789",
                                   // (20, 19) (wrong)
                                   "12345678901234567890.1234567890123456789"},
                                  {"1234567890123456789.123456789123456789",
                                   "12345678901234567890.123456789110000000",
                                   "1234567890123456789.123456789123456789",
                                   "1234567890123456789.123456789012345679",
                                   "123456789012345678.012345678901234568",
                                   "12345678901234567890.123456789012345679"}),

        };

        for (auto type_pair : arithmetic_scala_field_types) {
            auto type = std::get<0>(type_pair);
            DataTypePtr data_type_ptr;
            if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 27, 9);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
                // decimal32(7, 2)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 9, 2);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL64) {
                // decimal64(18, 9)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 18, 9);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
                // decimal128I(38,18)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 38, 18);
            } else {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            auto col = data_type_ptr->create_column();

            // serde for data types with default FormatOption
            DataTypeSerDe::FormatOptions default_format_option;
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();

            auto ser_col = ColumnString::create();
            ser_col->reserve(std::get<1>(type_pair).size());
            VectorBufferWriter buffer_writer(*ser_col.get());

            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                string test_str = std::get<1>(type_pair)[i];
                std::cout << "the str : " << test_str << std::endl;
                Slice rb_test(test_str.data(), test_str.size());
                // deserialize
                Status st = serde->deserialize_one_cell_from_hive_text(*col, rb_test,
                                                                       default_format_option);
                if (std::get<2>(type_pair)[i].empty()) {
                    EXPECT_EQ(st.ok(), false);
                    std::cout << "deserialize failed: " << st.to_json() << std::endl;
                    continue;
                }
                EXPECT_EQ(st.ok(), true);
                // serialize
                serde->serialize_one_cell_to_hive_text(*col, i, buffer_writer,
                                                       default_format_option);
                buffer_writer.commit();
                EXPECT_EQ(ser_col->get_data_at(ser_col->size() - 1).to_string(),
                          std::get<2>(type_pair)[i]);
            }
        }
    }

    // date and datetime type
    {
        typedef std::pair<FieldType, string> FieldType_RandStr;
        std::vector<FieldType_RandStr> date_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATE, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATE, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATEV2, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIME, "2020-01-01 12:00:00"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                                  "2020-01-01 12:00:00.666666"),
        };
        for (auto pair : date_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::unique_ptr<WrapperField> min_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));

            min_wf->set_to_min();
            max_wf->set_to_max();
            EXPECT_EQ(rand_wf->from_string(pair.second, 0, 0).ok(), true);

            string min_s = min_wf->to_string();
            string max_s = max_wf->to_string();
            string rand_date = rand_wf->to_string();

            Slice min_rb(min_s.data(), min_s.size());
            Slice max_rb(max_s.data(), max_s.size());
            Slice rand_rb(rand_date.data(), rand_date.size());

            auto col = data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            // make use c++ lib equals to wrapper field from_string behavior
            DataTypeSerDe::FormatOptions formatOptions;
            formatOptions.date_olap_format = true;

            Status st = serde->deserialize_one_cell_from_json(*col, min_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, max_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, rand_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(3);
            VectorBufferWriter buffer_writer(*ser_col.get());
            st = serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_one_cell_to_json(*col, 1, buffer_writer, formatOptions);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_one_cell_to_json(*col, 2, buffer_writer, formatOptions);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_date);
            StringRef min_s_d = ser_col->get_data_at(0);
            StringRef max_s_d = ser_col->get_data_at(1);
            StringRef rand_s_d = ser_col->get_data_at(2);

            std::cout << "min(" << min_s << ") with data_type_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with data_type_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_date << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(min_s, min_s_d.to_string());
            EXPECT_EQ(max_s, max_s_d.to_string());
            EXPECT_EQ(rand_date, rand_s_d.to_string());
        }
    }

    // ipv4 and ipv6 type
    {
        typedef std::pair<FieldType, string> FieldType_RandStr;
        std::vector<FieldType_RandStr> date_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "127.0.0.1"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6, "2405:9800:9800:66::2")};
        for (auto pair : date_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::unique_ptr<WrapperField> min_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));

            min_wf->set_to_min();
            max_wf->set_to_max();
            EXPECT_EQ(rand_wf->from_string(pair.second, 0, 0).ok(), true);

            string min_s = min_wf->to_string();
            string max_s = max_wf->to_string();
            string rand_ip = rand_wf->to_string();

            Slice min_rb(min_s.data(), min_s.size());
            Slice max_rb(max_s.data(), max_s.size());
            Slice rand_rb(rand_ip.data(), rand_ip.size());

            auto col = data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            // make use c++ lib equals to wrapper field from_string behavior
            DataTypeSerDe::FormatOptions formatOptions;

            Status st = serde->deserialize_one_cell_from_json(*col, min_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, max_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, rand_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(3);
            VectorBufferWriter buffer_writer(*ser_col.get());
            st = serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_one_cell_to_json(*col, 1, buffer_writer, formatOptions);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_one_cell_to_json(*col, 2, buffer_writer, formatOptions);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_ip);
            StringRef min_s_d = ser_col->get_data_at(0);
            StringRef max_s_d = ser_col->get_data_at(1);
            StringRef rand_s_d = ser_col->get_data_at(2);

            std::cout << "min(" << min_s << ") with data_type_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with data_type_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_ip << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(min_s, min_s_d.to_string());
            EXPECT_EQ(max_s, max_s_d.to_string());
            EXPECT_EQ(rand_ip, rand_s_d.to_string());
        }
    }

    // nullable data type with const column
    {
        DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
        DataTypePtr nullable_ptr = std::make_shared<DataTypeNullable>(data_type_ptr);
        std::unique_ptr<WrapperField> rand_wf(
                WrapperField::create_by_type(FieldType::OLAP_FIELD_TYPE_STRING));
        std::string test_str = generate(128);
        EXPECT_EQ(rand_wf->from_string(test_str, 0, 0).ok(), true);
        Field string_field(test_str);
        ColumnPtr col = nullable_ptr->create_column_const(0, string_field);
        DataTypeSerDe::FormatOptions default_format_option;
        DataTypeSerDeSPtr serde = nullable_ptr->get_serde();
        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        Status st =
                serde->serialize_one_cell_to_json(*col, 0, buffer_writer, default_format_option);
        EXPECT_EQ(st.ok(), true);
        buffer_writer.commit();
        StringRef rand_s_d = ser_col->get_data_at(0);
        EXPECT_EQ(rand_wf->to_string(), rand_s_d.to_string());
    }
}

TEST(CsvSerde, ComplexTypeSerdeCsvTest) {
    { // map<int,map<string,int>>
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';

        string str =
                "10\003\"key10\"\005100\004\"abcd\"\0051\002100\003\"key100\"\005100\004\"abcd\""
                "\0052\0021000\003\"ke"
                "y1000\"\0051000\004\"abcd\"\0053";

        DataTypePtr data_type_ptr = std::make_shared<DataTypeMap>(
                make_nullable(std::make_shared<DataTypeInt32>()),
                make_nullable(std::make_shared<DataTypeMap>(
                        make_nullable(std::make_shared<DataTypeString>()),
                        make_nullable(std::make_shared<DataTypeInt32>()))));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        EXPECT_EQ(st, Status::OK());

        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        serde->serialize_one_cell_to_hive_text(*col, 0, buffer_writer, formatOptions);
        buffer_writer.commit();
        StringRef rand_s_d = ser_col->get_data_at(0);
        std::cout << "test:" << str << std::endl;
        std::cout << "result : " << rand_s_d << std::endl;
        EXPECT_EQ(str, rand_s_d.to_string());
    }

    { //array<array<map<int,string>>>
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';

        string str = "500\005\"true\"\00410\005\"true\"\004100\005\"true\"";

        DataTypePtr data_type_ptr = make_nullable(std::make_shared<DataTypeArray>(make_nullable(
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeMap>(
                        make_nullable(std::make_shared<DataTypeInt32>()),
                        make_nullable(std::make_shared<DataTypeString>())))))));
        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        EXPECT_EQ(st, Status::OK());

        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        serde->serialize_one_cell_to_hive_text(*col, 0, buffer_writer, formatOptions);
        buffer_writer.commit();
        StringRef rand_s_d = ser_col->get_data_at(0);
        std::cout << "test:" << str << std::endl;
        std::cout << "result : " << rand_s_d << std::endl;
        EXPECT_EQ(str, rand_s_d.to_string());
    }
    { //struct<int,map<int,string>,struct<string,string,int>,array<int>>
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';

        string str =
                "5\0023\004\"value3\"\0034\004\"value4\"\0035\004\"value5\"\002\"true\"\003\"false"
                "\"\0037\0021\0032\003"
                "3";
        DataTypes substruct_dataTypes;
        substruct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        substruct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        substruct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeInt32>()));

        DataTypes struct_dataTypes;
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeInt32>()));
        struct_dataTypes.push_back(make_nullable(
                std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeInt32>()),
                                              make_nullable(std::make_shared<DataTypeString>()))));
        struct_dataTypes.push_back(
                make_nullable(std::make_shared<DataTypeStruct>(substruct_dataTypes)));
        struct_dataTypes.push_back(make_nullable(
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()))));

        DataTypePtr data_type_ptr =
                make_nullable(std::make_shared<DataTypeStruct>(struct_dataTypes));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        //        EXPECT_EQ(st, Status::OK());

        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        serde->serialize_one_cell_to_hive_text(*col, 0, buffer_writer, formatOptions);
        buffer_writer.commit();
        StringRef rand_s_d = ser_col->get_data_at(0);
        std::cout << "test:" << str << std::endl;
        std::cout << "result : " << rand_s_d << std::endl;
        EXPECT_EQ(str, rand_s_d.to_string());
    }
    { //map<int,struct<string,string>>
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';

        string str = "6\003\"false\"\004\"example\"";
        DataTypes substruct_dataTypes;
        substruct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        substruct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));

        DataTypePtr data_type_ptr = make_nullable(std::make_shared<DataTypeMap>(
                make_nullable(std::make_shared<DataTypeInt32>()),
                make_nullable(std::make_shared<DataTypeStruct>(substruct_dataTypes))));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        EXPECT_EQ(st, Status::OK());

        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        serde->serialize_one_cell_to_hive_text(*col, 0, buffer_writer, formatOptions);
        buffer_writer.commit();
        StringRef rand_s_d = ser_col->get_data_at(0);
        std::cout << "test:" << str << std::endl;
        std::cout << "result : " << rand_s_d << std::endl;
        EXPECT_EQ(str, rand_s_d.to_string());
    }
}

// Test for struct deserialization when data has fewer fields than schema
// This can happen due to Hive schema evolution (adding new columns to struct)
TEST(CsvSerde, StructHiveTextDeserializeFewerFields) {
    {
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';
        // Initialize null_format for the fix to work properly
        formatOptions.null_format = "\\N";
        formatOptions.null_len = 2;

        // Data only has 2 fields (field1, field2) but schema expects 3 fields
        // The third field should be filled with null
        string str = "hello\002world";

        DataTypes struct_dataTypes;
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeInt32>()));

        DataTypePtr data_type_ptr =
                make_nullable(std::make_shared<DataTypeStruct>(struct_dataTypes));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();

        // This should not crash - previously it would cause SIGSEGV due to out-of-bounds access
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        EXPECT_EQ(st, Status::OK());

        // Verify the column has 1 row
        EXPECT_EQ(col->size(), 1);

        std::cout << "StructHiveTextDeserializeFewerFields: Successfully deserialized struct with "
                     "fewer fields than schema"
                  << std::endl;
    }

    // Test case: array<struct<string, string, int>> where struct data has fewer fields
    {
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';
        // Initialize null_format for the fix to work properly
        formatOptions.null_format = "\\N";
        formatOptions.null_len = 2;

        // Array with one struct element, struct has only 2 fields but schema expects 3
        // For array<struct>, array uses \002 as delimiter (level 1), struct uses \003 (level 2)
        string str = "hello\003world";

        DataTypes struct_dataTypes;
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeInt32>()));

        DataTypePtr struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(struct_dataTypes));
        DataTypePtr data_type_ptr =
                make_nullable(std::make_shared<DataTypeArray>(struct_type));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();

        // This should not crash
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        EXPECT_EQ(st, Status::OK());

        // Verify the column has 1 row (1 array element)
        EXPECT_EQ(col->size(), 1);

        std::cout << "StructHiveTextDeserializeFewerFields: Successfully deserialized "
                     "array<struct> with fewer fields than schema"
                  << std::endl;
    }

    // Test case: struct with only one field in data, schema expects 2
    {
        DataTypeSerDe::FormatOptions formatOptions;
        formatOptions.collection_delim = '\002';
        formatOptions.map_key_delim = '\003';
        // Initialize null_format for the fix to work properly
        formatOptions.null_format = "\\N";
        formatOptions.null_len = 2;

        // Data has only 1 field, schema expects 2 fields
        // The second field should be filled with null
        string str = "only_one_field";

        DataTypes struct_dataTypes;
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeInt32>()));

        DataTypePtr data_type_ptr =
                make_nullable(std::make_shared<DataTypeStruct>(struct_dataTypes));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();

        // This should succeed with the fix - missing field is padded with null
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, formatOptions);
        EXPECT_EQ(st, Status::OK());

        // Verify the column has 1 row
        EXPECT_EQ(col->size(), 1);

        std::cout << "StructHiveTextDeserializeFewerFields: Successfully deserialized struct with "
                     "single field when schema expects two"
                  << std::endl;
    }
}

// Test for EncloseCsvLineReaderContext with zero/one column separators
TEST(CsvSerde, EncloseCsvLineReaderContextZeroColSepTest) {
    {
        std::string line_delimiter = "\n";
        std::string column_sep = ",";
        size_t col_sep_num = 0;  // This simulates empty _file_slot_descs

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        // Verify the context is created successfully
        EXPECT_NE(ctx, nullptr);

        // Test refresh - should work without issues
        ctx->refresh();

        std::cout << "EncloseCsvLineReaderContextZeroColSepTest: col_sep_num=0 handled correctly"
                  << std::endl;
    }

    {
        std::string line_delimiter = "\n";
        std::string column_sep = ",";
        size_t col_sep_num = 1;

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        EXPECT_NE(ctx, nullptr);
        ctx->refresh();

        std::cout << "EncloseCsvLineReaderContextZeroColSepTest: col_sep_num=1 handled correctly"
                  << std::endl;
    }

    {
        std::string line_delimiter = "\n";
        std::string column_sep = ",";
        size_t col_sep_num = 5;  // 6 columns means 5 separators

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        EXPECT_NE(ctx, nullptr);
        ctx->refresh();

        // Test reading a simple line
        std::string test_line = "a,b,c,d,e,f\n";
        const uint8_t* result =
                ctx->read_line(reinterpret_cast<const uint8_t*>(test_line.data()), test_line.size());

        // Should find the newline delimiter
        EXPECT_NE(result, nullptr);

        std::cout << "EncloseCsvLineReaderContextZeroColSepTest: Normal case handled correctly"
                  << std::endl;
    }
}

// Test for FormatOptions default values
TEST(CsvSerde, FormatOptionsDefaultValuesTest) {
    // Test case 1: Verify default values are set correctly
    {
        DataTypeSerDe::FormatOptions options;

        // Verify null_format default value
        EXPECT_STREQ(options.null_format, "\\N");
        EXPECT_EQ(options.null_len, 2);

        // Verify nested_string_wrapper default value
        EXPECT_STREQ(options.nested_string_wrapper, "");
        EXPECT_EQ(options.wrapper_len, 0);

        // Verify other default values
        EXPECT_EQ(options.date_olap_format, false);
        EXPECT_EQ(options.field_delim, ",");
        EXPECT_EQ(options.collection_delim, ',');
        EXPECT_EQ(options.map_key_delim, ':');
        EXPECT_EQ(options.converted_from_string, false);
        EXPECT_EQ(options.escape_char, 0);
        EXPECT_EQ(options._output_object_data, true);

        std::cout << "FormatOptionsDefaultValuesTest: Default values verified correctly"
                  << std::endl;
    }

    // Test case 2: Use FormatOptions with default values for serialization/deserialization
    {
        DataTypeSerDe::FormatOptions options;  // Use default values, don't set null_format

        // Create a nullable string type
        DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
        DataTypePtr nullable_ptr = std::make_shared<DataTypeNullable>(data_type_ptr);

        auto col = nullable_ptr->create_column();
        DataTypeSerDeSPtr serde = nullable_ptr->get_serde();

        // Test deserializing a normal value
        std::string test_str = "hello_world";
        Slice slice(test_str.data(), test_str.size());
        Status st = serde->deserialize_one_cell_from_json(*col, slice, options);
        EXPECT_EQ(st.ok(), true);
        EXPECT_EQ(col->size(), 1);

        // Test serializing back
        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        st = serde->serialize_one_cell_to_json(*col, 0, buffer_writer, options);
        EXPECT_EQ(st.ok(), true);
        buffer_writer.commit();

        StringRef result = ser_col->get_data_at(0);
        EXPECT_EQ(result.to_string(), test_str);

        std::cout << "FormatOptionsDefaultValuesTest: Serialization with defaults works correctly"
                  << std::endl;
    }

    // Test case 3: Test struct deserialization with default FormatOptions
    {
        DataTypeSerDe::FormatOptions options;  // Use default values only
        options.collection_delim = '\002';
        options.map_key_delim = '\003';
        // Note: null_format and null_len use default values

        // struct<string, int> with only one field in data
        std::string str = "test_value";

        DataTypes struct_dataTypes;
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeString>()));
        struct_dataTypes.push_back(make_nullable(std::make_shared<DataTypeInt32>()));

        DataTypePtr data_type_ptr =
                make_nullable(std::make_shared<DataTypeStruct>(struct_dataTypes));

        auto col = data_type_ptr->create_column();
        Slice slice(str.data(), str.size());
        DataTypeSerDeSPtr serde = data_type_ptr->get_serde();

        // This should work with default null_format values
        Status st = serde->deserialize_one_cell_from_hive_text(*col, slice, options);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(col->size(), 1);

        std::cout << "FormatOptionsDefaultValuesTest: Struct with default null_format works"
                  << std::endl;
    }
}


TEST(CsvSerde, EncloseCsvLineReaderContextEdgeCasesTest) {
    // Test case 1: Empty input with col_sep_num = 0
    {
        std::string line_delimiter = "\n";
        std::string column_sep = ",";
        size_t col_sep_num = 0;

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        ctx->refresh();

        // Test with empty input
        std::string empty_line = "";
        const uint8_t* result =
                ctx->read_line(reinterpret_cast<const uint8_t*>(empty_line.data()), empty_line.size());

        // Should return nullptr for empty input
        EXPECT_EQ(result, nullptr);

        std::cout << "EncloseCsvLineReaderContextEdgeCasesTest: Empty input handled correctly"
                  << std::endl;
    }

    // Test case 2: Single column with enclose character
    {
        std::string line_delimiter = "\n";
        std::string column_sep = ",";
        size_t col_sep_num = 0;  // Single column means 0 separators

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        ctx->refresh();

        // Test with enclosed single value
        std::string test_line = "\"hello world\"\n";
        const uint8_t* result =
                ctx->read_line(reinterpret_cast<const uint8_t*>(test_line.data()), test_line.size());

        EXPECT_NE(result, nullptr);

        std::cout << "EncloseCsvLineReaderContextEdgeCasesTest: Single enclosed column works"
                  << std::endl;
    }

    // Test case 3: Line with only delimiter (edge case)
    {
        std::string line_delimiter = "\n";
        std::string column_sep = ",";
        size_t col_sep_num = 0;

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        ctx->refresh();

        std::string test_line = "\n";
        const uint8_t* result =
                ctx->read_line(reinterpret_cast<const uint8_t*>(test_line.data()), test_line.size());

        // Should find the newline at position 0
        EXPECT_NE(result, nullptr);
        EXPECT_EQ(result, reinterpret_cast<const uint8_t*>(test_line.data()));

        std::cout << "EncloseCsvLineReaderContextEdgeCasesTest: Empty line handled correctly"
                  << std::endl;
    }

    // Test case 4: Multi-byte line delimiter with col_sep_num = 0
    {
        std::string line_delimiter = "\r\n";
        std::string column_sep = ",";
        size_t col_sep_num = 0;

        auto ctx = std::make_shared<EncloseCsvLineReaderContext>(
                line_delimiter, line_delimiter.size(), column_sep, column_sep.size(), col_sep_num,
                '"', '\\', false);

        ctx->refresh();

        std::string test_line = "value\r\n";
        const uint8_t* result =
                ctx->read_line(reinterpret_cast<const uint8_t*>(test_line.data()), test_line.size());

        EXPECT_NE(result, nullptr);

        std::cout << "EncloseCsvLineReaderContextEdgeCasesTest: Multi-byte delimiter works"
                  << std::endl;
    }
}
} // namespace doris::vectorized