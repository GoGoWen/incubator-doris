#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Build Step: Command Line

#####################################################################################
## deploy.sh content ##

doris_build_dir="$(cd $(dirname $0); pwd)/../.."
source "${doris_build_dir}"/regression-test/custom/doris-utils.sh

if [[ -z "${doris_build_dir}" ]]; then echo "ERROR: env doris_build_dir not set" && exit 1; fi

echo "#### Deploy Doris ####"
DORIS_HOME="${doris_build_dir}/output"
export DORIS_HOME

if [[ -z "${DORIS_HOME}/fe" ]]; then echo "ERROR: NO FE" && exit 1; fi
if [[ -z "${DORIS_HOME}/fe" ]]; then echo "ERROR: NO BE" && exit 1; fi
exit_flag=0

(
    set -e
    echo "#### 1. install java and mysql"
    install_java8
    install_mysql
    
    echo "#### 2. start Doris EF"
    if ! start_doris_fe; then echo "ERROR: Start doris fe failed." && exit 1; fi

    echo "#### 3. start Doris BE"
    if ! start_doris_be; then echo "ERROR: Start doris be failed." && exit 1; fi

    echo "#### 4. ADD BE TO FE"
    if ! add_doris_be_to_fe; then echo "ERROR: Add doris be failed." && exit 1; fi
)
exit_flag="$?"

echo "#### . check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    stop_doris
    print_doris_fe_log
    print_doris_be_log
fi

exit "${exit_flag}"
