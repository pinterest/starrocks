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

#include "storage/index/inverted/clucene/csv_analyzer.h"

#include <boost/locale/encoding_utf.hpp>

#include <string>
#include <vector>

#include "gutil/strings/split.h"

namespace starrocks {

namespace {
constexpr int32_t READ_BUFFER_SIZE = 4096;
}

// CSV Tokenizer that splits text on commas using RFC 4180 CSV parsing
class CsvTokenizer : public lucene::analysis::Tokenizer {
private:
    std::vector<std::wstring> _tokens;
    std::vector<std::wstring>::const_iterator _current_token;
    int32_t _current_offset;

    void initialize(lucene::util::Reader* reader);

public:
    explicit CsvTokenizer(lucene::util::Reader* reader);
    ~CsvTokenizer() override = default;

    lucene::analysis::Token* next(lucene::analysis::Token* token) override;
    void reset(lucene::util::Reader* reader);
};

void CsvTokenizer::initialize(lucene::util::Reader* reader) {
    // Read all text from the reader
    const TCHAR* buffer_ptr;
    std::wstring wide_text;
    int32_t chars_read;
    while ((chars_read = reader->read(buffer_ptr, 1, READ_BUFFER_SIZE)) > 0) {
        wide_text.append(buffer_ptr, chars_read);
    }

    // Convert to UTF-8 for CSV parsing
    std::string text = boost::locale::conv::utf_to_utf<char>(wide_text);

    // Parse CSV with proper escaping (RFC 4180: quoted fields and "" for quotes)
    std::vector<std::string> temp_tokens;
    SplitCSVLineWithDelimiterForStrings(text, ',', &temp_tokens);

    // Pre-convert all tokens to wide strings once (more efficient than converting on each next() call)
    _tokens.clear();
    _tokens.reserve(temp_tokens.size());
    for (const auto& token : temp_tokens) {
        _tokens.emplace_back(boost::locale::conv::utf_to_utf<wchar_t>(token));
    }

    _current_token = _tokens.begin();
    _current_offset = 0;
}

CsvTokenizer::CsvTokenizer(lucene::util::Reader* reader) : _current_offset(0) {
    initialize(reader);
}

lucene::analysis::Token* CsvTokenizer::next(lucene::analysis::Token* token) {
    if (_current_token == _tokens.end()) {
        return nullptr;
    }

    // Token is already a wide string - no conversion needed
    const std::wstring& token_str = *_current_token++;
    token->set(token_str.c_str(), _current_offset, _current_offset + token_str.length());
    _current_offset += token_str.length() + 1; // +1 for the comma separator
    return token;
}

void CsvTokenizer::reset(lucene::util::Reader* reader) {
    _tokens.clear();
    _current_offset = 0;
    initialize(reader);
}

lucene::analysis::TokenStream* CsvAnalyzer::tokenStream(const TCHAR* /*fieldName*/, lucene::util::Reader* reader) {
    return _CLNEW CsvTokenizer(reader);
}

lucene::analysis::TokenStream* CsvAnalyzer::reusableTokenStream(const TCHAR* /*fieldName*/,
                                                                lucene::util::Reader* reader) {
    lucene::analysis::Tokenizer* tokenizer = static_cast<lucene::analysis::Tokenizer*>(getPreviousTokenStream());
    if (tokenizer == nullptr) {
        tokenizer = _CLNEW CsvTokenizer(reader);
        setPreviousTokenStream(tokenizer);
    } else {
        tokenizer->reset(reader);
    }
    return tokenizer;
}

} // namespace starrocks
