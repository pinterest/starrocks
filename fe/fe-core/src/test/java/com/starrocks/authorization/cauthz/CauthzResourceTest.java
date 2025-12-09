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

import com.starrocks.authorization.cauthz.starrocks.CauthzStarRocksResource;
import com.starrocks.catalog.InternalCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CauthzResourceTest {
    @Test
    public void testBasic() {
        CauthzAccessResourceImpl cauthzResource = CauthzStarRocksResource.builder().setSystem().build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("system"));
        Assertions.assertEquals("*", cauthzResource.getValue("system"));

        cauthzResource = CauthzStarRocksResource.builder().setUser("u1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("user"));
        Assertions.assertEquals("u1", cauthzResource.getValue("user"));

        cauthzResource = CauthzStarRocksResource.builder().setCatalog("c1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("catalog"));
        Assertions.assertEquals("c1", cauthzResource.getValue("catalog"));

        cauthzResource = CauthzStarRocksResource.builder().setCatalog("c1").setDatabase("d1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("database"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("catalog"));
        Assertions.assertEquals("c1", cauthzResource.getValue("catalog"));
        Assertions.assertEquals("d1", cauthzResource.getValue("database"));

        cauthzResource = CauthzStarRocksResource.builder().setCatalog("c1").setDatabase("d1").setTable("t1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("database"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("catalog"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("table"));
        Assertions.assertEquals("c1", cauthzResource.getValue("catalog"));
        Assertions.assertEquals("d1", cauthzResource.getValue("database"));
        Assertions.assertEquals("t1", cauthzResource.getValue("table"));

        cauthzResource = CauthzStarRocksResource.builder()
                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setView("v1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("database"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("view"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("catalog"));
        Assertions.assertEquals("default_catalog", cauthzResource.getValue("catalog"));
        Assertions.assertEquals("d1", cauthzResource.getValue("database"));
        Assertions.assertEquals("v1", cauthzResource.getValue("view"));

        cauthzResource = CauthzStarRocksResource.builder()
                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setMaterializedView("mv1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("database"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("catalog"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("materialized_view"));
        Assertions.assertEquals("default_catalog", cauthzResource.getValue("catalog"));
        Assertions.assertEquals("d1", cauthzResource.getValue("database"));
        Assertions.assertEquals("mv1", cauthzResource.getValue("materialized_view"));

        cauthzResource = CauthzStarRocksResource.builder()
                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setFunction("f1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("database"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("catalog"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("function"));
        Assertions.assertEquals("default_catalog", cauthzResource.getValue("catalog"));
        Assertions.assertEquals("d1", cauthzResource.getValue("database"));
        Assertions.assertEquals("f1", cauthzResource.getValue("function"));

        cauthzResource = CauthzStarRocksResource.builder().setGlobalFunction("gf1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("global_function"));
        Assertions.assertEquals("gf1", cauthzResource.getValue("global_function"));

        cauthzResource = CauthzStarRocksResource.builder().setResource("r1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("resource"));
        Assertions.assertEquals("r1", cauthzResource.getValue("resource"));

        cauthzResource = CauthzStarRocksResource.builder().setResourceGroup("rg1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("resource_group"));
        Assertions.assertEquals("rg1", cauthzResource.getValue("resource_group"));

        cauthzResource = CauthzStarRocksResource.builder().setStorageVolume("sv1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("storage_volume"));
        Assertions.assertEquals("sv1", cauthzResource.getValue("storage_volume"));

        cauthzResource = CauthzStarRocksResource.builder().setDatabase("d1").setPipe("p1").build();
        Assertions.assertTrue(cauthzResource.getKeys().contains("database"));
        Assertions.assertTrue(cauthzResource.getKeys().contains("pipe"));
        Assertions.assertEquals("d1", cauthzResource.getValue("database"));
        Assertions.assertEquals("p1", cauthzResource.getValue("pipe"));
    }

    @Test
    public void testResourceToString() {
        CauthzAccessResourceImpl cauthzResource = CauthzStarRocksResource.builder()
                .setCatalog("c1").setDatabase("d1").setTable("t1").build();
        String str = cauthzResource.toString();
        Assertions.assertTrue(str.contains("catalog=c1"));
        Assertions.assertTrue(str.contains("database=d1"));
        Assertions.assertTrue(str.contains("table=t1"));
    }

    @Test
    public void testContainsKey() {
        CauthzAccessResourceImpl cauthzResource = CauthzStarRocksResource.builder()
                .setCatalog("c1").setDatabase("d1").build();
        Assertions.assertTrue(cauthzResource.containsKey("catalog"));
        Assertions.assertTrue(cauthzResource.containsKey("database"));
        Assertions.assertFalse(cauthzResource.containsKey("table"));
        Assertions.assertFalse(cauthzResource.containsKey("nonexistent"));
    }
}

