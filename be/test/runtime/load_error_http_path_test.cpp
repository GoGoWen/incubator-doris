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
#include <string>
#include <sstream>

#include "common/config.h"
#include "service/backend_options.h"
#include "runtime/fragment_mgr.h"

namespace doris {

    class LoadErrorHttpPathTest : public testing::Test {
    };

    TEST_F(LoadErrorHttpPathTest, EmptyFileName
    ) {
    EXPECT_EQ(to_load_error_http_path(""),
    "");
}

TEST_F(LoadErrorHttpPathTest, BasicUrlConstruction
) {
config::proxy_for_errurl = "proxy/";
config::webserver_port = 8080;

std::string result = to_load_error_http_path("error.log");
std::stringstream ss;
ss << "proxy/http://" << BackendOptions::get_localhost() << ":8080/api/_load_error_log?file=error.log";
std::string expect = ss.str();
EXPECT_EQ(result, expect);
}

TEST_F(LoadErrorHttpPathTest, NoProxyPrefix
) {
config::proxy_for_errurl = "";
config::webserver_port = 80;

std::string result = to_load_error_http_path("test.log");
std::stringstream ss;
ss << "http://" << BackendOptions::get_localhost() << ":80/api/_load_error_log?file=test.log";
std::string expect = ss.str();
EXPECT_EQ(result, expect);
}

TEST_F(LoadErrorHttpPathTest, SpecialCharsInFileName
) {
config::proxy_for_errurl = "proxy/";
config::webserver_port = 443;

std::string result = to_load_error_http_path("error 2023.log");
std::stringstream ss;
ss << "proxy/http://" << BackendOptions::get_localhost() << ":443/api/_load_error_log?file=error 2023.log";
std::string expect = ss.str();
EXPECT_EQ(result, expect);
}

TEST_F(LoadErrorHttpPathTest, DifferentPort
) {
config::proxy_for_errurl = "proxy/";
config::webserver_port = 1234;

std::string result = to_load_error_http_path("data.log");
std::stringstream ss;
ss << "proxy/http://" << BackendOptions::get_localhost() << ":1234/api/_load_error_log?file=data.log";
std::string expect = ss.str();
EXPECT_EQ(result, expect);
}

TEST_F(LoadErrorHttpPathTest, LongFileName
) {
config::proxy_for_errurl = "";
config::webserver_port = 80;

std::string long_name(256, 'a');  // 长文件名测试
std::string result = to_load_error_http_path(long_name);
EXPECT_EQ(result, "http://" + BackendOptions::get_localhost() + ":80/api/_load_error_log?file=" + long_name);
}

}