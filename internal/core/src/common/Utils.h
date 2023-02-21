// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <google/protobuf/text_format.h>

#include <string>

#include "common/Consts.h"
#include "config/ConfigChunkManager.h"
#include "exceptions/EasyAssert.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"

namespace milvus {
inline DatasetPtr
GenDataset(const int64_t nb, const int64_t dim, const void* xb) {
    return knowhere::GenDataSet(nb, dim, xb);
}

inline const float*
GetDatasetDistance(const DatasetPtr& dataset) {
    return dataset->GetDistance();
}

inline const int64_t*
GetDatasetIDs(const DatasetPtr& dataset) {
    return dataset->GetIds();
}

inline int64_t
GetDatasetRows(const DatasetPtr& dataset) {
    return dataset->GetRows();
}

inline const void*
GetDatasetTensor(const DatasetPtr& dataset) {
    return dataset->GetTensor();
}

inline int64_t
GetDatasetDim(const DatasetPtr& dataset) {
    return dataset->GetDim();
}

inline const size_t*
GetDatasetLims(const DatasetPtr& dataset) {
    return dataset->GetLims();
}

inline bool
PrefixMatch(const std::string& str, const std::string& prefix) {
    auto ret = strncmp(str.c_str(), prefix.c_str(), prefix.length());
    if (ret != 0) {
        return false;
    }

    return true;
}

inline DatasetPtr
GenResultDataset(const int64_t nq, const int64_t topk, const int64_t* ids, const float* distance) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(nq);
    ret_ds->SetDim(topk);
    ret_ds->SetIds(ids);
    ret_ds->SetDistance(distance);
    ret_ds->SetIsOwner(true);
    return ret_ds;
}

inline bool
PostfixMatch(const std::string& str, const std::string& postfix) {
    if (postfix.length() > str.length()) {
        return false;
    }

    int offset = str.length() - postfix.length();
    auto ret = strncmp(str.c_str() + offset, postfix.c_str(), postfix.length());
    if (ret != 0) {
        return false;
    }
    //
    //    int i = postfix.length() - 1;
    //    int j = str.length() - 1;
    //    for (; i >= 0; i--, j--) {
    //        if (postfix[i] != str[j]) {
    //            return false;
    //        }
    //    }
    return true;
}

inline int64_t
upper_align(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups * align;
}

inline int64_t
upper_div(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups;
}

inline bool
IsMetricType(const std::string& str, const knowhere::MetricType& metric_type) {
    return !strcasecmp(str.c_str(), metric_type.c_str());
}

inline bool
PositivelyRelated(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::IP);
}

inline std::string
MatchKnowhereError(knowhere::Status status) {
    switch (status) {
        case knowhere::Status::invalid_args:
            return "err: invalid args";
        case knowhere::Status::invalid_param_in_json:
            return "err: invalid param in json";
        case knowhere::Status::out_of_range_in_json:
            return "err: out of range in json";
        case knowhere::Status::type_conflict_in_json:
            return "err: type conflict in json";
        case knowhere::Status::invalid_metric_type:
            return "err: invalid metric type";
        case knowhere::Status::empty_index:
            return "err: empty index";
        case knowhere::Status::not_implemented:
            return "err: not implemented";
        case knowhere::Status::index_not_trained:
            return "err: index not trained";
        case knowhere::Status::index_already_trained:
            return "err: index already trained";
        case knowhere::Status::faiss_inner_error:
            return "err: faiss inner error";
        case knowhere::Status::annoy_inner_error:
            return "err: annoy inner error";
        case knowhere::Status::hnsw_inner_error:
            return "err: hnsw inner error";
        case knowhere::Status::malloc_error:
            return "err: malloc error";
        case knowhere::Status::diskann_inner_error:
            return "err: diskann inner error";
        case knowhere::Status::diskann_file_error:
            return "err: diskann file error";
        case knowhere::Status::invalid_value_in_json:
            return "err: invalid value in json";
        case knowhere::Status::arithmetic_overflow:
            return "err: arithmetic overflow";
        default:
            return "not match the error type in knowhere";
    }
}

}  // namespace milvus
