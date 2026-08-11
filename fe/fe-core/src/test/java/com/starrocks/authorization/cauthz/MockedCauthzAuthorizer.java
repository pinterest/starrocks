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

/**
 * Mock implementation of CauthzAuthorizer for testing purposes.
 * This allows tests to control authorization decisions without
 * requiring an actual cauthz service.
 */
public class MockedCauthzAuthorizer implements CauthzAuthorizer {
    private boolean allowAll = true;

    public MockedCauthzAuthorizer() {
    }

    public MockedCauthzAuthorizer(boolean allowAll) {
        this.allowAll = allowAll;
    }

    @Override
    public void init() throws Exception {
        // No initialization needed for mock
    }

    @Override
    public boolean authorize(CauthzStarRocksAccessRequest request) {
        return allowAll;
    }

    public void setAllowAll(boolean allowAll) {
        this.allowAll = allowAll;
    }

    public boolean isAllowAll() {
        return allowAll;
    }
}

