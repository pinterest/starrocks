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

#pragma once

#include <CLucene.h>
#include <CLucene/analysis/Analyzers.h>

namespace starrocks {

// CSV Analyzer that splits text on commas
class CsvAnalyzer : public lucene::analysis::Analyzer {
public:
    CsvAnalyzer() = default;
    ~CsvAnalyzer() override = default;

    lucene::analysis::TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override;
    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                                       lucene::util::Reader* reader) override;
    
    // Required by some CLucene versions
    using lucene::analysis::Analyzer::tokenStream;
};

} // namespace starrocks
