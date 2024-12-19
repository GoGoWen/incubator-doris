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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetLdapPassVar;
import org.apache.doris.analysis.SetNamesVar;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetTransaction;
import org.apache.doris.analysis.SetUserDefinedVar;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.IAMUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Set executor
public class SetExecutor {
    private static final Logger LOG = LogManager.getLogger(SetExecutor.class);

    private ConnectContext ctx;
    private SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariable(SetVar var) throws DdlException {
        if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            ctx.getEnv().getAuth().setPassword(setPassVar);
        } else if (var instanceof SetLdapPassVar) {
            SetLdapPassVar setLdapPassVar = (SetLdapPassVar) var;
            ctx.getEnv().getAuth().setLdapPassword(setLdapPassVar);
        } else if (var instanceof SetNamesVar) {
            // do nothing
            return;
        } else if (var instanceof SetTransaction) {
            // do nothing
            return;
        } else if (var instanceof SetUserDefinedVar) {
            ConnectContext.get().setUserVar(var);
        } else if (var.getVariable().equalsIgnoreCase("erp")) {
            if (ConnectContext.get().getBdpAuthContext() != null
                    && IAMUtil.isSourceInWhitelist(ConnectContext.get().getBdpAuthContext().getSource())) {
                ConnectContext.get().getBdpAuthContext().setErp(var.getValue().getStringValue());
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_OPERATION_FOR_SOURCE_NOT_IN_WHITELIST,
                        var.getVariable());
            }
            ConnectContext.get().getBdpAuthContext().setErpChanged(true);
            ConnectContext.get().getBdpAuthContext().setUserType(null);
            ConnectContext.get().getBdpAuthContext().setBusinessLine(null);
            LOG.info("succeed to set erp, " + ConnectContext.get().getBdpAuthContext().toString());
        } else if (var.getVariable().equalsIgnoreCase("hadoop_user_name")) {
            if (ConnectContext.get().getBdpAuthContext() != null
                    && IAMUtil.isSourceInWhitelist(ConnectContext.get().getBdpAuthContext().getSource())) {
                if (!ConnectContext.get().getBdpAuthContext().isErpChanged()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_OPERATION_FOR_ERP_NO_CHANGED,
                            var.getVariable());
                }
                String hadoopUserName = var.getValue().getStringValue();
                String erp = ConnectContext.get().getBdpAuthContext().getErp();
                String userToken = IAMUtil.getUserTokenByHadoopUserName(erp, hadoopUserName);
                if (userToken == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_OPERATION_FOR_CALL_IAM_ERROR,
                            var.getVariable());
                }
                ConnectContext.get().getBdpAuthContext().setHadoopUserName(hadoopUserName);
                ConnectContext.get().getBdpAuthContext().setUserToken(userToken);
                ConnectContext.get().getBdpAuthContext().setErpChanged(false);
                LOG.info("succeed to set hadoop_user_name, " + ConnectContext.get().getBdpAuthContext().toString());
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_OPERATION_FOR_SOURCE_NOT_IN_WHITELIST,
                        var.getVariable());
            }
        } else if (var.getVariable().equalsIgnoreCase("businessline")) {
            if (ConnectContext.get().getBdpAuthContext() != null
                    && IAMUtil.isSourceInWhitelist(ConnectContext.get().getBdpAuthContext().getSource())) {
                if (!ConnectContext.get().getBdpAuthContext().isErpChanged()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_OPERATION_FOR_ERP_NO_CHANGED,
                            var.getVariable());
                }
                String businessLineName = var.getValue().getStringValue();
                ConnectContext.get().getBdpAuthContext().setBusinessLine(businessLineName);
                ConnectContext.get().getBdpAuthContext().setUserType("dev_personal");
                LOG.info("succeed to set businessline, " + ConnectContext.get().getBdpAuthContext().toString());
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_OPERATION_FOR_SOURCE_NOT_IN_WHITELIST,
                        var.getVariable());
            }
        } else {
            VariableMgr.setVar(ctx.getSessionVariable(), var);
        }
    }

    public void execute() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            setVariable(var);
        }
    }
}
