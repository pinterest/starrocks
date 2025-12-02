// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authorization.cauthz.starrocks;

import com.starrocks.analysis.TableName;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.cauthz.CauthzAccessController;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.PipeName;

import static java.util.Locale.ENGLISH;

/*
 * CauthzStarRocksAccessController is used to check whether the current user
 * has the specified privilege to act on a resource. The check methods in this class exist
 * to obtain authorization decisions on differing chains of object types
 * such as catalog, database, table, view, function, etc.
 */
public class CauthzStarRocksAccessController extends CauthzAccessController {
    public CauthzStarRocksAccessController() {
        super();
    }

    @Override
    public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setSystem().build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setUser(impersonateUser.getUser()).build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnCatalog(ConnectContext context, String catalogName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();

        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();

        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyTable(ConnectContext context, String catalog, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(context, catalog, db);
        for (Table table : database.getTables()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(catalog)
                                .setDatabase(database.getFullName())
                                .setTable(table.getName())
                                .build(),
                        context.getCurrentUserIdentity(),
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(CauthzStarRocksResource.builder()
                        .setCatalog(tableName.getCatalog())
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .setColumn(column)
                        .build(),
                context.getCurrentUserIdentity(), privilegeType);
    }

    @Override
    public void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyView(ConnectContext context, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db);
        for (Table table : database.getViews()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setView(table.getName())
                                .build(),
                        context.getCurrentUserIdentity(),
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnMaterializedView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(ConnectContext context, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db);
        for (Table table : database.getMaterializedViews()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setMaterializedView(table.getName())
                                .build(),
                        context.getCurrentUserIdentity(),
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkFunctionAction(ConnectContext context, Database database, Function function,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database.getFullName())
                        .setFunction(function.getSignature())
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnFunction(ConnectContext context, String database, Function function)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database)
                        .setFunction(function.getSignature())
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyFunction(ConnectContext context, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db);
        for (Function function : database.getFunctions()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setFunction(function.getSignature())
                                .build(),
                        context.getCurrentUserIdentity(),
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkGlobalFunctionAction(ConnectContext context, Function function,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnGlobalFunction(ConnectContext context, Function function)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    @Override
    public void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db);
        for (Table table : database.getTables()) {
            if (table.isOlapView()) {
                checkViewAction(context, new TableName(database.getFullName(), table.getName()), privilegeType);
            } else if (table.isMaterializedView()) {
                checkMaterializedViewAction(context,
                        new TableName(database.getFullName(), table.getName()), privilegeType);
            } else {
                checkTableAction(context, new TableName(database.getFullName(), table.getName()), privilegeType);
            }
        }
    }

    @Override
    public void checkResourceAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnResource(ConnectContext context, String name) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkResourceGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setResourceGroup(name)
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkPipeAction(ConnectContext context, PipeName name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setDatabase(name.getDbName())
                        .setPipe(name.getPipeName())
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnPipe(ConnectContext context, PipeName pipeName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setDatabase(pipeName.getDbName())
                        .setPipe(pipeName.getPipeName())
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkStorageVolumeAction(ConnectContext context, String storageVolume,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                context.getCurrentUserIdentity(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolume)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                context.getCurrentUserIdentity(),
                PrivilegeType.ANY);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        return normalizePrivilegeName(privilegeType);
    }

    public static String normalizePrivilegeName(PrivilegeType privilegeType) {
        // Converts the enum value (e.g. CREATE_TABLE or CREATE TABLE) to lower_snake_case
        return privilegeType.name().replaceAll("\\s+", "_").toLowerCase(ENGLISH);
    }
}
