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

package com.starrocks.authorization.cauthz;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.NativeAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.cauthz.starrocks.CauthzStarRocksAccessController;
import com.starrocks.authorization.cauthz.starrocks.CauthzStarRocksResource;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.UserIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CauthzInterfaceTest {

    @BeforeAll
    public static void beforeClass() {
        // Set the cauthz authorization class name to use our mock
        Config.cauthz_authorization_class_name =
                "com.starrocks.authorization.cauthz.MockedCauthzAuthorizer";
    }

    @Test
    public void testAccessControlProvider() {
        AccessControlProvider accessControlProvider = new AccessControlProvider(null,
                new NativeAccessController());
        accessControlProvider.removeAccessControl("test_catalog");

        accessControlProvider.setAccessControl("test_catalog", new NativeAccessController());
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("test_catalog")
                instanceof NativeAccessController);
        accessControlProvider.removeAccessControl("test_catalog");

        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("test_catalog")
                instanceof NativeAccessController);
    }

    @Test
    public void testCauthzAccessRequest() {
        CauthzAccessResourceImpl resource = CauthzStarRocksResource.builder()
                .setCatalog("catalog1")
                .setDatabase("db1")
                .setTable("table1")
                .build();

        CauthzStarRocksAccessRequest request = new CauthzStarRocksAccessRequest(
                resource,
                UserIdentity.ROOT,
                "select");

        Assertions.assertEquals(resource, request.getResource());
        // getUser() returns the user name, not the full UserIdentity string
        Assertions.assertEquals("root", request.getUser());
        Assertions.assertEquals("select", request.getAccessType());

        // Test toString
        String str = request.toString();
        Assertions.assertTrue(str.contains("select"));
        Assertions.assertTrue(str.contains("root"));
    }

    @Test
    public void testConvertToAccessType() {
        CauthzStarRocksAccessController controller = new CauthzStarRocksAccessController();

        Assertions.assertEquals("select", controller.convertToAccessType(PrivilegeType.SELECT));
        Assertions.assertEquals("insert", controller.convertToAccessType(PrivilegeType.INSERT));
        Assertions.assertEquals("update", controller.convertToAccessType(PrivilegeType.UPDATE));
        Assertions.assertEquals("delete", controller.convertToAccessType(PrivilegeType.DELETE));
        Assertions.assertEquals("drop", controller.convertToAccessType(PrivilegeType.DROP));
        Assertions.assertEquals("alter", controller.convertToAccessType(PrivilegeType.ALTER));
        Assertions.assertEquals("create_table",
                controller.convertToAccessType(PrivilegeType.CREATE_TABLE));
        Assertions.assertEquals("create_view",
                controller.convertToAccessType(PrivilegeType.CREATE_VIEW));
        Assertions.assertEquals("create_database",
                controller.convertToAccessType(PrivilegeType.CREATE_DATABASE));
        Assertions.assertEquals("operate", controller.convertToAccessType(PrivilegeType.OPERATE));
        Assertions.assertEquals("usage", controller.convertToAccessType(PrivilegeType.USAGE));
        Assertions.assertEquals("refresh", controller.convertToAccessType(PrivilegeType.REFRESH));
    }

    @Test
    public void testNormalizePrivilegeName() {
        Assertions.assertEquals("select",
                CauthzStarRocksAccessController.normalizePrivilegeName(PrivilegeType.SELECT));
        Assertions.assertEquals("create_table",
                CauthzStarRocksAccessController.normalizePrivilegeName(PrivilegeType.CREATE_TABLE));
        Assertions.assertEquals("create_database",
                CauthzStarRocksAccessController.normalizePrivilegeName(PrivilegeType.CREATE_DATABASE));
        Assertions.assertEquals("create_materialized_view",
                CauthzStarRocksAccessController.normalizePrivilegeName(
                        PrivilegeType.CREATE_MATERIALIZED_VIEW));
    }

    @Test
    public void testMockedCauthzAuthorizer() {
        // Test the mock authorizer directly
        MockedCauthzAuthorizer authorizer = new MockedCauthzAuthorizer();
        Assertions.assertTrue(authorizer.isAllowAll());

        CauthzAccessResourceImpl resource = CauthzStarRocksResource.builder()
                .setSystem().build();
        CauthzStarRocksAccessRequest request = new CauthzStarRocksAccessRequest(
                resource, UserIdentity.ROOT, "operate");

        // Should return true when allowAll is true (default)
        Assertions.assertTrue(authorizer.authorize(request));

        // Should return false when allowAll is false
        authorizer.setAllowAll(false);
        Assertions.assertFalse(authorizer.authorize(request));

        // Test constructor with allowAll parameter
        MockedCauthzAuthorizer authorizerDeny = new MockedCauthzAuthorizer(false);
        Assertions.assertFalse(authorizerDeny.authorize(request));

        MockedCauthzAuthorizer authorizerAllow = new MockedCauthzAuthorizer(true);
        Assertions.assertTrue(authorizerAllow.authorize(request));
    }
}
