// Copyright 2018 The Bidb Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

#pragma once

#include <algorithm>
#include <roachpb/data.pb.h>
#include "db.h"
#include "encoding.h"
#include "iterator.h"
#include "keys.h"
#include "protos/storage/engine/enginepb/mvcc.pb.h"
#include "status.h"
#include "timestamp.h"
#include "vec_filter.h"
#include <vector>
#include <stack>


namespace znbase {

    template <bool reverse> class vecScanner {
    public:
        vecScanner(DBIterator* iter, DBSlice start, DBSlice end, int64_t max_keys,DBTimestamp timestamp,roachpb::PushDownExpr push_expr,DBTxn txn,
                   bool inconsistent, bool tombstones, bool ignore_sequence, bool fail_on_more_recent,
                   DBVecResults &dbResults,roachpb::VecResults &vecResults,roachpb::VecResults &vecResults_filter)
                : iter_(iter),
                  iter_rep_(iter->rep.get()),
                  start_key_(ToSlice(start)),
                  end_key_(ToSlice(end)),
                  max_keys_(max_keys),
                  timestamp_(timestamp),
                  push_expr(push_expr),
                  txn_id_(ToSlice(txn.id)),
                  txn_epoch_(txn.epoch),
                  txn_sequence_(txn.sequence),
                  txn_max_timestamp_(txn.max_timestamp),
                  txn_ignored_seqnums_(txn.ignored_seqnums),
                  inconsistent_(inconsistent),
                  tombstones_(tombstones),
                  ignore_sequence_(ignore_sequence),
                  fail_on_more_recent_(fail_on_more_recent),
                  check_uncertainty_(timestamp < txn.max_timestamp),
                  dbResults(dbResults),vecResults(vecResults),vecResults_filter(vecResults_filter),
                  intents_(new rocksdb::WriteBatch),
                  most_recent_timestamp_(),
                  peeked_(false),
                  iters_before_seek_(kMaxItersBeforeSeek / 2)  {
            memset(&dbResults, 0, sizeof(dbResults));
            dbResults.status = kSuccess;
            rs_count = 0;
            iter_->intents.reset();
        }

        void scan() {
            // TODO(peter): Remove this timing/debugging code.
            // auto pctx = rocksdb::get_perf_context();
            // pctx->Reset();
            // auto start_time = std::chrono::steady_clock::now();
            // auto elapsed = std::chrono::steady_clock::now() - start_time;
            // auto micros = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            // printf("seek %d: %s\n", int(micros), pctx->ToString(true).c_str());

            if (reverse) {
                if (!iterSeekReverse(EncodeKey(start_key_, 0, 0))) {
                    return ;
                }
            } else {
                if (!iterSeek(EncodeKey(start_key_, 0, 0))) {
                    return ;
                }
            }

            initPushDown();
            while (getAndAdvance()) {
            }

            maybeFailOnMoreRecent();

            if (vecResults.count() == max_keys_ && advanceKey()) {
                if (reverse) {
                    // It is possible for cur_key_ to be pointing into mvccScanner.saved_buf_
                    // instead of iter_rep_'s underlying storage if iterating in reverse (see
                    // iterPeekPrev), so copy the key onto the DBIterator struct to ensure it
                    // has a lifetime that outlives the DBScanResults.
                    dbResults.resume_key.len = iter_->rev_resume_key.size();
                    dbResults.resume_key.data = static_cast<char*>(malloc(iter_->rev_resume_key.size()));
                    memcpy(dbResults.resume_key.data, iter_->rev_resume_key.data(), iter_->rev_resume_key.size());
                } else {
                    dbResults.resume_key.len = cur_key_.size();
                    dbResults.resume_key.data = static_cast<char*>(malloc(cur_key_.size()));
                    memcpy(dbResults.resume_key.data, cur_key_.data(), cur_key_.size());
                }
            }
            return fillResults();
        }

    private:
        void fillResults() {
            if (dbResults.status.len == 0){
                if (intents_->Count() > 0) {
                    dbResults.intents = ToDBSlice(intents_->Data());
                }
                iter_->intents.reset(intents_.release());
            }
        }

        //根据列id获取该列的类型
        int32_t getRequireType(int32_t col_id) {
            for (int i = 0; i < push_expr.col_ids_size(); i++) {
                if(col_id == push_expr.col_ids(i))
                    return push_expr.col_types(i);
            }
            return -1;
        }

        //每过滤完一次KV数据,清除vecResults_filter中的数据
        void clearValue(){
            for(int i=0;i<filterID_vec_.size();i++){
                znbase::roachpb::VecValue *col_values_res = vecResults_filter.mutable_col_values(i);
                znbase::roachpb::NullValue *null_values_res = vecResults_filter.mutable_null_values(i);
                null_values_res->clear_index();
                auto col_type = col_values_res->col_type();
                switch (col_type) {
                    case ColumnType_BOOL:
                        col_values_res->clear_v_int32();
                        break;
                    case ColumnType_INT:
                        col_values_res->clear_v_int();
                        break;
                    case ColumnType_DATE:
                        col_values_res->clear_v_int32();
                        break;
                    case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                        col_values_res->clear_values();
                        break;
                    case ColumnType_FLOAT:
                        col_values_res->clear_v_float();
                        break;
                    case ColumnType_DECIMAL:
                        col_values_res->clear_v_decimal();
                        break;
                    case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ:
                        col_values_res->clear_v_int();
                        break;
                    case ColumnType_TIME:
                        col_values_res->clear_v_int();
                        break;
                    case ColumnType_UUID:
                        col_values_res->clear_values();
                        break;
                }
            }
        }

        //预处理过滤条件
        void prePushdownExpr(znbase::roachpb::FilterUnion *filter_Union){
            if (filter_Union->has_base()) {
                // base节点
                std::string filter_Value = "0";
                struct Decimal filter_Decimal_Value;
                znbase::roachpb::FilterType filter_Type =  filter_Union->type();//过滤下推传下来的过滤类型
                znbase::roachpb::BaseFilter *filter_Base = filter_Union->mutable_base();//获取base过滤指针
                std::string str_ID = filter_Base->attribute();//过滤下推传下来的列ID
                int filter_ID = atoi(str_ID.c_str());//列ID类型转换
                int32_t column_Type = getRequireType(filter_ID);//根据列ID获取类型
                if(filter_Base->has_value()) {
                    filter_Value = filter_Base->value();//过滤条件比较的值
                }
                if(filter_Type != roachpb::In){//如果过滤条件类型是非IN类型,则将value值存在对应类型的vector中
                    switch (column_Type){
                        case ColumnType_BOOL:
                            break;
                        case ColumnType_INT:
                            col_filter_int64_.push_back(stoi(filter_Value));
                            break;
                        case ColumnType_DATE:
                            if(filter_Type == roachpb::IsNotNull || filter_Type == roachpb::IsNull){
                                col_filter_int64_.push_back(0);
                            }else{
                                col_filter_int64_.push_back(convertDateToDay(filter_Value));
                            }
                            break;
                        case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                            col_filter_string_.push_back(filter_Value);
                            break;
                        case ColumnType_FLOAT:
                            col_filter_double_.push_back(stod(filter_Value));
                            break;
                        case ColumnType_DECIMAL:{
                            PharseDecimal(filter_Value,filter_Decimal_Value);
                            col_filter_decimal_.push_back(filter_Decimal_Value);
                            break;
                        }
                        case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ:
                            if(filter_Type == roachpb::IsNotNull || filter_Type == roachpb::IsNull){
                                col_filter_int64_.push_back(0);
                            }else{
                                col_filter_int64_.push_back(convertDateTimeToDay(filter_Value));
                            }
                            break;
                        case ColumnType_TIME:
                            break;
                        case ColumnType_UUID:
                            col_filter_string_.push_back(filter_Value);
                            break;
                    }
                }else{//如果过滤条件类型是IN类型,则将values值存在对应类型的vector中
                    if(filter_Base->values_size() != 0) {
                        ::google::protobuf::RepeatedPtrField< ::std::string>* values = filter_Base->mutable_values();//IN中的values
                        filter_Values.clear();
                        if(values->size() != 0){
                            for (::google::protobuf::RepeatedPtrField< ::std::string>::iterator it = values->begin(); it != values->end(); ++it) {
                                filter_Values.push_back(*it);
                            }
                        }
                    }
                    switch (column_Type){
                        case ColumnType_BOOL:
                            break;
                        case ColumnType_INT:
                            filter_Int_Values.clear();
                            if (filter_Values.size() != 0) {
                                std::transform(filter_Values.begin(), filter_Values.end(),
                                               std::back_inserter(filter_Int_Values),
                                               [](const std::string &str) { return std::stoi(str);});
                            }
                            col_Int64_values_.push_back(filter_Int_Values);
                            break;
                        case ColumnType_DATE:
                            filter_Int_Values.clear();
                            if (filter_Values.size() != 0){
                                std::transform(filter_Values.begin(), filter_Values.end(),
                                               std::back_inserter(filter_Int_Values),
                                               [](const std::string &str) { return convertDateToDay(str); });
                            }
                            col_Int64_values_.push_back(filter_Int_Values);
                            break;
                        case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                            col_String_values_.push_back(filter_Values);
                            break;
                        case ColumnType_FLOAT:
                            filter_Float_Values.clear();
                            if (filter_Values.size() != 0) {
                                std::transform(filter_Values.begin(), filter_Values.end(),
                                               std::back_inserter(filter_Float_Values),
                                               [](const std::string &str) { return std::stod(str); });
                            }
                            col_Double_values_.push_back(filter_Float_Values);
                            break;
                        case ColumnType_DECIMAL:
                        {
                            filter_Decimal_Values.clear();
                            for(int i=0;i<filter_Values.size();i++){
                                filter_Decimal_Value.Coeff.abs.clear();
                                PharseDecimal(filter_Values[i],filter_Decimal_Value);
                                filter_Decimal_Values.push_back(filter_Decimal_Value);
                            }
                            col_Decimal_values_.push_back(filter_Decimal_Values);
                            break;
                        }
                        case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ:
                            filter_Int_Values.clear();
                            if (filter_Values.size() != 0){
                                std::transform(filter_Values.begin(), filter_Values.end(),
                                               std::back_inserter(filter_Int_Values),
                                               [](const std::string &str) { return convertDateTimeToDay(str);});
                            }
                            col_Int64_values_.push_back(filter_Int_Values);
                            break;
                        case ColumnType_TIME:
                            break;
                        case ColumnType_UUID:
                            col_String_values_.push_back(filter_Values);
                            break;
                    }
                }
                if(std::find(value_DecodeID_vec_.begin(), value_DecodeID_vec_.end(), filter_ID) == value_DecodeID_vec_.end()){//require column中添加过滤条件中的列id
                    value_DecodeID_vec_.push_back(filter_ID);
                }
                if(std::find(filterID_vec_.begin(), filterID_vec_.end(), filter_ID) == filterID_vec_.end()){//记录过滤条件中涉及的列id
                    filterID_vec_.push_back(filter_ID);
                }
            } else if (filter_Union->has_logical()) {
                // 左子节点
                prePushdownExpr(filter_Union->mutable_logical()->mutable_left());
                // 右子节点
                prePushdownExpr(filter_Union->mutable_logical()->mutable_right());
            }
        }

        //后序遍历递归实现
        void PreOrderTraverse(znbase::roachpb::FilterUnion *filter_Union, std::stack<bool> *stack)
        {
            if (filter_Union->has_base()) {
                // base节点
                // 组织filter条件，对kv进行数据判断
                bool result;
                znbase::roachpb::FilterType filter_Type =  filter_Union->type();
                znbase::roachpb::BaseFilter *filter_Base = filter_Union->mutable_base();
                std::string str_ID = filter_Base->attribute();//比较的id
                int filter_ID = atoi(str_ID.c_str());//比较的id转成int
                int col_Type = getRequireType(filter_ID);//类型
                if(std::find(filterID_used_.begin(),filterID_used_.end(),filter_ID) == filterID_used_.end()){
                    filterID_used_.push_back(filter_ID);//已经过滤过的列id的集合
                }
                result = Filter_Base(filter_ID,col_Type,filter_Type);
                stack->push(result);
            } else if (filter_Union->has_logical()) {
                // 左子节点
                PreOrderTraverse(filter_Union->mutable_logical()->mutable_left(), stack);
                // 右子节点
                PreOrderTraverse(filter_Union->mutable_logical()->mutable_right(), stack);
                // logic根节点
                // 判断logic filter是and还是or
                if (filter_Union->type() == znbase::roachpb::FilterType::And) {
                    bool left_result = stack->top();
                    stack->pop();
                    bool right_result = stack->top();
                    stack->pop();
                    stack->push(left_result && right_result);
                } else if (filter_Union->type() == znbase::roachpb::FilterType::Or) {
                    bool left_result = stack->top();
                    stack->pop();
                    bool right_result = stack->top();
                    stack->pop();
                    stack->push(left_result || right_result);
                }else if (filter_Union->type() == znbase::roachpb::FilterType::Not) {
                    bool left_result = stack->top();
                    stack->pop();
                    stack->push(!left_result);
                }
            }
        }

        //对pushdown的预处理
        void initPushDown(){
            vecResults.clear_col_values();
            vecResults.clear_col_ids();
            vecResults.clear_count();
            vecResults.clear_null_values();
            primaryID_vec_.clear();
            value_DecodeID_vec_.clear();
            if(push_expr.required_cols_size() != 0){
                value_DecodeID_vec_ = std::vector<int>(push_expr.required_cols_size());
            }
            for (int i = 0; i < push_expr.required_cols_size(); i++) {
                auto col_id = push_expr.required_cols(i);
                value_DecodeID_vec_[i] = col_id;
                vecResults.add_col_ids(col_id);
                znbase::roachpb::VecValue *col_values = vecResults.add_col_values();
                col_values->set_col_id(col_id);
                col_values->set_col_type(getRequireType(col_id));
                vecResults.add_null_values();
            }
//            std::cout << "push_expr.filters_size() == " << push_expr.filters_size() << std::endl;
            if(push_expr.filters_size()!=0){
                col_filter_int64_.clear();
                col_filter_double_.clear();
                col_filter_string_.clear();
                col_filter_decimal_.clear();
                col_String_values_.clear();
                col_Int64_values_.clear();
                col_Double_values_.clear();
                col_Decimal_values_.clear();
                for(int i = 0;i < push_expr.filters_size();i++){
                    znbase::roachpb::FilterUnion *filter_union = push_expr.mutable_filters(i);
                    prePushdownExpr(filter_union);//递归添加过滤条件中的列id
                }
                for(int i=0;i<filterID_vec_.size();i++){
                    auto col_ID= filterID_vec_[i];
                    mapFilter_[col_ID] = i;//建立col_id和vecvaluefilter index的map
                    vecResults_filter.add_col_ids(col_ID);
                    znbase::roachpb::VecValue *col_Values_Filter = vecResults_filter.add_col_values();
                    col_Values_Filter->set_col_id(col_ID);
                    col_Values_Filter->set_col_type(getRequireType(col_ID));
                    vecResults_filter.add_null_values();
                }
            }
            if(push_expr.primary_cols_size() != 0){
                primaryID_vec_ = std::vector<int>(push_expr.primary_cols_size());
            }
            for(int i=0;i<push_expr.primary_cols_size();i++){
                auto Primary_direct = 0;
                auto Primary_ID = push_expr.primary_cols(i);
                if(push_expr.primary_cols_direct_size() != 0){
                    Primary_direct = push_expr.primary_cols_direct(i);
                }
                primaryID_vec_[i] = Primary_ID;
                mapPrimary_[Primary_ID] = Primary_direct;//建立主键ID和主键顺序ASC DESC的映射a
                if(std::find(value_DecodeID_vec_.begin(), value_DecodeID_vec_.end(), Primary_ID) != value_DecodeID_vec_.end()){
                    auto iter = std::remove(value_DecodeID_vec_.begin(), value_DecodeID_vec_.end(), Primary_ID);//如果value_DecodeID_vec_包括主键,则去掉该id并更改size
                    value_DecodeID_vec_.erase(iter, value_DecodeID_vec_.end());
                }
            }
        }

        bool seqNumIsIgnored(int32_t sequence) const {
            // The ignored seqnum ranges are guaranteed to be
            // non-overlapping, non-contiguous, and guaranteed to be
            // sorted in seqnum order. We're going to look from the end to
            // see if the current intent seqnum is ignored.
            //
            // TODO(nvanbenschoten): this can use use binary search to improve
            // the complexity. Worth looking into if this loop takes a while, due to
            // long lists of ignored sequence where the ones near the specified sequence
            // number are near the start. Until then, the current implementation is
            // simpler and correct.
            for (int i = txn_ignored_seqnums_.len - 1; i >= 0; i--) {
                if (sequence < txn_ignored_seqnums_.ranges[i].start_seqnum) {
                    // The history entry's sequence number is lower/older than
                    // the current ignored range. Go to the previous range
                    // and try again.
                    continue;
                }
                // Here we have a range where the start seqnum is lower than the current
                // intent seqnum. Does it include it?
                if (sequence > txn_ignored_seqnums_.ranges[i].end_seqnum) {
                    // Here we have a range where the current history entry's seqnum
                    // is higher than the range's end seqnum. Given that the
                    // ranges are storted, we're guaranteed that there won't
                    // be any further overlapping range at a lower value of i.
                    return false;
                }
                // Yes, it's included. We're going to skip over this
                // intent seqnum and retry the search above.
                return true;
            }
            // Exhausted the ignore list. Not ignored.
            return false;
        }

        bool getFromIntentHistory() {
            znbase::storage::engine::enginepb::MVCCMetadata_SequencedIntent readIntent;
            readIntent.set_sequence(txn_sequence_);

            auto end = meta_.intent_history().end();
            znbase::storage::engine::enginepb::MVCCMetadata_SequencedIntent intent;

            // Look for the intent with the sequence number less than or equal to the
            // read sequence. To do so, search using upper_bound, which returns an
            // iterator pointing to the first element in the range [first, last) that is
            // greater than value, or last if no such element is found. Then, return the
            // previous value.
            auto up = std::upper_bound(
                    meta_.intent_history().begin(), end, readIntent,
                    [](const znbase::storage::engine::enginepb::MVCCMetadata_SequencedIntent& a,
                       const znbase::storage::engine::enginepb::MVCCMetadata_SequencedIntent& b) -> bool {
                        return a.sequence() < b.sequence();
                    });

            while (up != meta_.intent_history().begin()) {
                const auto intent_pos = up - 1;
                // Here we have found a history entry with the highest seqnum that's
                // equal or lower to the txn seqnum.
                //
                // However this entry may also be part of an ignored range
                // (partially rolled back). We'll check this next. If it is,
                // we'll try the previous sequence in the intent history.
                if (seqNumIsIgnored(intent_pos->sequence())) {
                    // This entry was part of an ignored range. Iterate back in intent
                    // history to the previous sequence, and check if that one
                    // is ignored.
                    up--;
                    continue;
                }
                // This history entry has not been ignored, so we're going to
                // select this version.
                intent = *intent_pos;
                break;
            }

            if (up == meta_.intent_history().begin()) {
                // It is possible that no intent exists such that the sequence is less
                // than the read sequence. In this case, we cannot read a value from the
                // intent history.
                return false;
            }

            rocksdb::Slice value = intent.value();
            if (value.size() > 0 || tombstones_) {
                // If we're adding a value due to a previous intent, as indicated by the
                // zero-valued timestamp, we want to populate the timestamp as of current
                // metaTimestamp. Note that this may be controversial as this maybe be
                // neither the write timestamp when this intent was written. However, this
                // was the only case in which a value could have been returned from a read
                // without an MVCC timestamp.
                if (cur_timestamp_ == kZeroTimestamp) {
                    auto meta_timestamp = meta_.timestamp();
                    auto key = EncodeKey(cur_key_,
                                         meta_timestamp.wall_time(),
                                         meta_timestamp.logical());
//                    kvs_->Put(key, value);
                } else {
//                    kvs_->Put(cur_raw_key_, value);
                }
            }
            return true;
        }

        void maybeFailOnMoreRecent() {
            if (dbResults.status.len != 0 || most_recent_timestamp_ == kZeroTimestamp) {
                return;
            }
            dbResults.write_too_old_timestamp = most_recent_timestamp_;
            intents_->Clear();
        }

        bool uncertaintyError(DBTimestamp ts) {
            dbResults.uncertainty_timestamp = ts;
            intents_->Clear();
            return false;
        }

        bool setStatus(const DBStatus& status) {
            dbResults.status = status;
            return false;
        }

        bool getAndAdvance() {
            const bool is_value = cur_timestamp_ != kZeroTimestamp;

            if (is_value) {
                // ts < read_ts
                if (timestamp_ > cur_timestamp_) {
                    // 1. Fast path: 没有意图，我们的读取时间戳比最新版本的时间戳更新。
                    return addAndAdvance(cur_value_);
                }

                // ts == read_ts
                if (timestamp_ == cur_timestamp_) {
                    if (fail_on_more_recent_) {
                        // 2. 我们的txn的read timestamp等于最新版本的时间戳，
                        // 并且扫描器被配置为在相等或更新的版本上抛出write too old错误。
                        // 将当前时间戳与我们看到的最大时间戳合并，这样我们就知道返回错误，
                        // 但是继续扫描，这样我们就可以返回最大可能的时间
                        if (cur_timestamp_ > most_recent_timestamp_) {
                            most_recent_timestamp_ = cur_timestamp_;
                        }
                        return advanceKey();
                    }

                    // 3. 没有意图，我们的读取时间戳等于最新版本的时间戳。
                    return addAndAdvance(cur_value_);
                }

                if (fail_on_more_recent_) {
                    // 4. 我们的txn的read时间戳小于最新版本的时间戳，
                    // 并且扫描器被配置为在相同或更新的版本上抛出write too old错误。
                    // 将当前时间戳与我们看到的最大时间戳合并，这样我们就知道返回错误，
                    // 但是继续扫描，这样我们就可以返回最大可能的时间。
                    if (cur_timestamp_ > most_recent_timestamp_) {
                        most_recent_timestamp_ = cur_timestamp_;
                    }
                    return advanceKey();
                }

                if (check_uncertainty_) {
                    // 5. 我们的txn的读取时间戳小于txn看到的max时间戳。
                    // 我们需要检查时钟不确定度误差。
                    if (txn_max_timestamp_ >= cur_timestamp_) {
                        return uncertaintyError(cur_timestamp_);
                    }
                    // Delegate to seekVersion to return a clock uncertainty error
                    // if there are any more versions above txn_max_timestamp_.
                    return seekVersion(txn_max_timestamp_, true);
                }

                // 6. Our txn's read timestamp is greater than or equal to the
                // max timestamp seen by the txn so clock uncertainty checks are
                // unnecessary. We need to seek to the desired version of the
                // value (i.e. one with a timestamp earlier than our read
                // timestamp).
                return seekVersion(timestamp_, false);
            }

            if (cur_value_.size() == 0) {
                return setStatus(FmtStatus("zero-length mvcc metadata"));
            }

            if (!meta_.ParseFromArray(cur_value_.data(), cur_value_.size())) {
                return setStatus(FmtStatus("unable to decode MVCCMetadata"));
            }

            if (meta_.has_raw_bytes()) {
                // 7. Emit immediately if the value is inline.
                return addAndAdvance(meta_.raw_bytes());
            }

            if (!meta_.has_txn()) {
                return setStatus(FmtStatus("intent without transaction"));
            }

            const bool own_intent = (meta_.txn().id() == txn_id_);
            const DBTimestamp meta_timestamp = ToDBTimestamp(meta_.timestamp());
            // meta_timestamp is the timestamp of an intent value, which we may or may
            // not end up ignoring, depending on factors codified below. If we do ignore
            // the intent then we want to read at a lower timestamp that's strictly
            // below the intent timestamp (to skip the intent), but also does not exceed
            // our read timestamp (to avoid erroneously picking up future committed
            // values); this timestamp is prev_timestamp.
            const DBTimestamp prev_timestamp =
                    timestamp_ < meta_timestamp ? timestamp_ : PrevTimestamp(meta_timestamp);
            // Intents for other transactions are visible at or below:
            //   max(txn.max_timestamp, read_timestamp)
            const DBTimestamp max_visible_timestamp = check_uncertainty_ ? txn_max_timestamp_ : timestamp_;
            // ... unless we're intending on failing on more recent writes,
            // in which case other transaction's intents are always visible.
            const bool other_intent_visible =
                    max_visible_timestamp >= meta_timestamp || fail_on_more_recent_;
            if (!own_intent && !other_intent_visible) {
                // 8. The key contains an intent, but we're reading before the
                // intent. Seek to the desired version. Note that if we own the
                // intent (i.e. we're reading transactionally) we want to read
                // the intent regardless of our read timestamp and fall into
                // case 8 below.
                return seekVersion(timestamp_, false);
            }

            if (inconsistent_) {
                // 9. The key contains an intent and we're doing an inconsistent
                // read at a timestamp newer than the intent. We ignore the
                // intent by insisting that the timestamp we're reading at is a
                // historical timestamp < the intent timestamp. However, we
                // return the intent separately; the caller may want to resolve
                // it.
                if (vecResults.count() == max_keys_) {
                    // We've already retrieved the desired number of keys and now
                    // we're adding the resume key. We don't want to add the
                    // intent here as the intents should only correspond to KVs
                    // that lie before the resume key.
                    return false;
                }
                intents_->Put(cur_raw_key_, cur_value_);
                return seekVersion(prev_timestamp, false);
            }

            if (!own_intent) {
                // 10. The key contains an intent which was not written by our
                // transaction and our read timestamp is newer than that of the
                // intent. Note that this will trigger an error on the Go
                // side. We continue scanning so that we can return all of the
                // intents in the scan range.
                intents_->Put(cur_raw_key_, cur_value_);
                return advanceKey();
            }

            if (txn_epoch_ == meta_.txn().epoch()) {
                if (txn_sequence_ >= meta_.txn().sequence() && !seqNumIsIgnored(meta_.txn().sequence())) {
                    // 11. We're reading our own txn's intent at an equal or higher sequence.
                    // Note that we read at the intent timestamp, not at our read timestamp
                    // as the intent timestamp may have been pushed forward by another
                    // transaction. Txn's always need to read their own writes.
                    return seekVersion(meta_timestamp, false);
                } else {
                    // 12. We're reading our own txn's intent at a lower sequence than is
                    // currently present in the intent. This means the intent we're seeing
                    // was written at a higher sequence than the read and that there may or
                    // may not be earlier versions of the intent (with lower sequence
                    // numbers) that we should read. If there exists a value in the intent
                    // history that has a sequence number equal to or less than the read
                    // sequence, read that value.
                    const bool found = getFromIntentHistory();
                    if (found) {
                        return advanceKey();
                    }
                    // 13. If no value in the intent history has a sequence number equal to
                    // or less than the read, we must ignore the intents laid down by the
                    // transaction all together. We ignore the intent by insisting that the
                    // timestamp we're reading at is a historical timestamp < the intent
                    // timestamp.
                    return seekVersion(prev_timestamp, false);
                }
            }

            if (txn_epoch_ < meta_.txn().epoch()) {
                // 14. We're reading our own txn's intent but the current txn has
                // an earlier epoch than the intent. Return an error so that the
                // earlier incarnation of our transaction aborts (presumably
                // this is some operation that was retried).
                return setStatus(FmtStatus("failed to read with epoch %u due to a write intent with epoch %u",
                                           txn_epoch_, meta_.txn().epoch()));
            }

            // 15. We're reading our own txn's intent but the current txn has a
            // later epoch than the intent. This can happen if the txn was
            // restarted and an earlier iteration wrote the value we're now
            // reading. In this case, we ignore the intent and read the
            // previous value as if the transaction were starting fresh.
            return seekVersion(prev_timestamp, false);
        }

        // nextKey advances the iterator to point to the next MVCC key
        // greater than cur_key_. Returns false if the iterator is exhausted
        // or an error occurs.
        bool nextKey() {
            key_buf_.assign(cur_key_.data(), cur_key_.size());

            for (int i = 0; i < iters_before_seek_; ++i) {
                if (!iterNext()) {
                    return false;
                }
                if (cur_key_ != key_buf_) {
                    iters_before_seek_ = std::max<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
                    return true;
                }
            }

            // We're pointed at a different version of the same key. Fall back
            // to seeking to the next key. We append 2 NULs to account for the
            // "next-key" and a trailing zero timestamp. See EncodeKey and
            // SplitKey for more details on the encoded key format.
            iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
            key_buf_.append("\0\0", 2);
            return iterSeek(key_buf_);
        }

        // backwardLatestVersion backs up the iterator to the latest version
        // for the specified key. The parameter i is used to maintain the
        // iteration count between the loop here and the caller (usually
        // prevKey). Returns false if an error occurred.
        bool backwardLatestVersion(const rocksdb::Slice& key, int i) {
            key_buf_.assign(key.data(), key.size());

            for (; i < iters_before_seek_; ++i) {
                rocksdb::Slice peeked_key;
                if (!iterPeekPrev(&peeked_key)) {
                    // No previous entry exists, so we're at the latest version of
                    // key.
                    return true;
                }
                if (peeked_key != key_buf_) {
                    // The key changed which means the current key is the latest
                    // version.
                    iters_before_seek_ = std::max<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
                    return true;
                }
                if (!iterPrev()) {
                    return false;
                }
            }

            iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
            key_buf_.append("\0", 1);
            return iterSeek(key_buf_);
        }

        // prevKey backs up the iterator to point to the prev MVCC key less
        // than the specified key. Returns false if the iterator is
        // exhausted or an error occurs.
        bool prevKey(const rocksdb::Slice& key) {
            key_buf_.assign(key.data(), key.size());

            for (int i = 0; i < iters_before_seek_; ++i) {
                rocksdb::Slice peeked_key;
                if (!iterPeekPrev(&peeked_key)) {
                    return false;
                }
                if (peeked_key != key_buf_) {
                    return backwardLatestVersion(peeked_key, i + 1);
                }
                if (!iterPrev()) {
                    return false;
                }
            }

            iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
            key_buf_.append("\0", 1);
            return iterSeekReverse(key_buf_);
        }

        // advanceKey advances the iterator to point to the next MVCC
        // key. Returns false if the iterator is exhausted or an error
        // occurs.
        bool advanceKey() {
            if (reverse) {
                return prevKey(cur_key_);
            } else {
                return nextKey();
            }
        }

        bool advanceKeyAtEnd() {
            if (reverse) {
                // Iterating to the next key might have caused the iterator to
                // reach the end of the key space. If that happens, back up to
                // the very last key.
                clearPeeked();
                iter_rep_->SeekToLast();
                if (!updateCurrent()) {
                    return false;
                }
                return advanceKey();
            } else {
                // We've reached the end of the iterator and there is nothing
                // left to do.
                return false;
            }
        }

        bool advanceKeyAtNewKey(const rocksdb::Slice& key) {
            if (reverse) {
                // We've advanced to the next key but need to move back to the
                // previous key.
                return prevKey(key);
            } else {
                // We're already at the new key so there is nothing to do.
                return true;
            }
        }

        //处理filter操作
        bool Filter_Base(int col_id,int col_type,roachpb::FilterType filter_Type){
            int index = mapFilter_[col_id];//获取列id对应的col_va`lue
            znbase::roachpb::VecValue *col_values_filter = vecResults_filter.mutable_col_values(index);
            znbase::roachpb::NullValue *null_values_filter = vecResults_filter.mutable_null_values(index);
            int64_t col_Int_Value;
            std::string col_Str_Value;
            double col_Double_Value;
            struct Decimal col_Decimal_Value;
            struct Time col_Time_Value ;
            int64_t filter_Int_Value;
            std::string filter_Str_Value;
            double filter_Double_Value;
            struct Decimal filter_Decimal_Value;
            std::vector<int64_t> filter_Int_Values;
            std::vector<double> filter_Double_Values;
            std::vector<std::string> filter_Str_Values;
            int direct_Flag = 0;
            std::vector<struct Decimal> filter_Decimal_Values;
            if(std::find(primaryID_vec_.begin(),primaryID_vec_.end(),col_id) != primaryID_vec_.end()){
                std::vector<rocksdb::Slice> keys_filter = keys_;
                direct_Flag = mapPrimary_[col_id];
                switch (col_type){
                    case ColumnType_INT:{
                        if(filter_Type != roachpb::In){
                            filter_Int_Value = col_filter_int64_[count_int64];
                            count_int64++;
                        }else{
                            filter_Int_Values = col_Int64_values_[count_int64_values];
                            count_int64_values++;
                        }
                        if(direct_Flag == 1){
                            DecodeVarintDescending(keys_filter[col_id], &col_Int_Value);
                        }else{
                            DecodeVarintAscending(keys_filter[col_id], &col_Int_Value);
                        }
                        col_values_filter->add_v_int(col_Int_Value);
                        null_values_filter->add_index(false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Int_Value,filter_Int_Values);
                            default:
                                return true;
                        }
                    }
                    case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:{
                        col_Str_Value.clear();
                        filter_Str_Value.clear();
                        if(filter_Type != roachpb::In){
                            filter_Str_Value = col_filter_string_[count_string];
                            count_string++;
                        }else{
                            filter_Str_Values = col_String_values_[count_string_values];
                            count_string_values++;
                        }
                        if(direct_Flag == 1){
                            DecodeUnsafeStringDescending(keys_filter[col_id],&col_Str_Value);
                        } else{
                            DecodeUnsafeStringAscending(keys_filter[col_id],&col_Str_Value);
                        }
                        col_values_filter->add_values(col_Str_Value);
                        null_values_filter->add_index( false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Str_Value,filter_Str_Values);
                            case roachpb::FilterType::StringContains:
                                return StringContains(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringEnds:
                                return StringEnds(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringStarts:
                                return StringStarts(col_Str_Value,filter_Str_Value);
                            default:
                                return true;
                        }
                    }
                    case ColumnType_DECIMAL:{
                        if(filter_Type != roachpb::In){
                            filter_Decimal_Value.Coeff.abs.clear();
                            filter_Decimal_Value = col_filter_decimal_[count_decimal];
                            count_decimal++;
                        }else{
                            filter_Decimal_Values = col_Decimal_values_[count_decimal_values];
                            count_decimal_values++;
                        }
                        col_Decimal_Value.Coeff.abs.clear();
                        col_Decimal_Value.Negative = false;
                        if(direct_Flag == 1){
                            DecodeDecimalDescending(keys_filter[col_id],&col_Decimal_Value);
                        } else{
                            DecodeDecimalAscending(keys_filter[col_id],&col_Decimal_Value);
                        }
                        roachpb::Decimal *dec_Value = col_values_filter->add_v_decimal();
                        dec_Value->set_form(col_Decimal_Value._Form);
                        dec_Value->set_negative(col_Decimal_Value.Negative);
                        dec_Value->set_exponent(col_Decimal_Value.Exponent);
                        dec_Value->set_neg(col_Decimal_Value.Coeff.neg);
                        if(col_Decimal_Value.Coeff.abs.size() != 0){
                            dec_Value->add_abs(col_Decimal_Value.Coeff.abs[0]);
                        }
                        null_values_filter->add_index(false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) == 0){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::Greater:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) == 1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::GreaterOrEqual:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) != -1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::Less:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) == -1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::LessOrEqual:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) != 1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::In:{
                                for(int i=0;i<filter_Decimal_Values.size();i++){
                                    struct Decimal val = filter_Decimal_Values[i];
                                    if(CompareDecimals(col_Decimal_Value,val) == 0){
                                        return true;
                                    }
                                }
                                return false;
                            }
                            default:
                                return true;
                        }
                    }
                    case ColumnType_FLOAT:
                        if(filter_Type != roachpb::In){
                            filter_Double_Value = col_filter_double_[count_double];
                            count_double++;
                        }else{
                            filter_Double_Values = col_Double_values_[count_double_values];
                            count_double_values++;
                        }
                        if(direct_Flag == 1){
                            DecodeFloatDescending(keys_filter[col_id], &col_Double_Value);
                        }else{
                            DecodeFloatAscending(keys_filter[col_id], &col_Double_Value);
                        }
                        col_values_filter->add_v_double(col_Double_Value);
                        null_values_filter->add_index(false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Double_Value,filter_Double_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Double_Value,filter_Double_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Double_Value,filter_Double_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Double_Value,filter_Double_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Double_Value,filter_Double_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Double_Value,filter_Double_Values);
                            default:
                                return true;
                        }
                    case ColumnType_DATE:
                        if(filter_Type != roachpb::In){
                            filter_Int_Value = col_filter_int64_[count_int64];
                            count_int64++;
                        }else{
                            filter_Int_Values = col_Int64_values_[count_int64_values];
                            count_int64_values++;
                        }
                        if(direct_Flag == 1){
                            DecodeVarintDescending(keys_filter[col_id], &col_Int_Value);
                        }else{
                            DecodeVarintAscending(keys_filter[col_id], &col_Int_Value);
                        }
                        col_values_filter->add_v_int32(col_Int_Value);
                        null_values_filter->add_index(false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Int_Value,filter_Int_Values);
                            default:
                                return true;
                        }
                    case ColumnType_TIMESTAMP:
                        if(filter_Type != roachpb::In){
                            filter_Int_Value = col_filter_int64_[count_int64];
                            count_int64++;
                        }else{
                            filter_Int_Values = col_Int64_values_[count_int64_values];
                            count_int64_values++;
                        }
                        if(direct_Flag == 1){
                            DecodeVarintDescending(keys_filter[col_id], &col_Int_Value);
                        }else{
                            DecodeVarintAscending(keys_filter[col_id], &col_Int_Value);
                        }
                        col_Int_Value = col_Time_Value.t_timespec.tv_sec+col_Time_Value.t_timespec.tv_nsec/1e9;
                        col_values_filter->add_v_int(col_Int_Value);
                        null_values_filter->add_index( false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Int_Value,filter_Int_Values);
                            default:
                                return true;
                        }
                    case ColumnType_UUID:
                        col_Str_Value.clear();
                        filter_Str_Value.clear();
                        if(filter_Type != roachpb::In){
                            filter_Str_Value = col_filter_string_[count_string];
                            count_string++;
                        }else{
                            filter_Str_Values = col_String_values_[count_string_values];
                            count_string_values++;
                        }
                        if(direct_Flag == 1){
                            DecodeUnsafeStringDescending(keys_filter[col_id],&col_Str_Value);
                        } else{
                            DecodeUnsafeStringAscending(keys_filter[col_id],&col_Str_Value);
                        }
                        col_Str_Value = convUUIDToString((unsigned char *)col_Str_Value.c_str());//string转unsign char*
                        col_values_filter->add_values(col_Str_Value);
                        null_values_filter->add_index( false);
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Str_Value,filter_Str_Values);
                            case roachpb::FilterType::StringContains:
                                return StringContains(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringEnds:
                                return StringEnds(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringStarts:
                                return StringStarts(col_Str_Value,filter_Str_Value);
                            default:
                                return true;
                        }
                }
            }else{
                switch (col_type){
                    case ColumnType_INT:
                        if(filter_Type != roachpb::In){
                            filter_Int_Value = col_filter_int64_[count_int64];
                            count_int64++;
                        }else{
                            filter_Int_Values = col_Int64_values_[count_int64_values];
                            count_int64_values++;
                        }
                        if(!decode_Values_[col_id].empty()){
                            DecodeIntValue(decode_Values_[col_id], &col_Int_Value);
                            col_values_filter->add_v_int(col_Int_Value);
                            null_values_filter->add_index(false);
                        }else{
                            col_values_filter->add_v_int(0);
                            null_values_filter->add_index( true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Int_Value,filter_Int_Values);
                            default:
                                return true;
                        }
                    case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                        col_Str_Value.clear();
                        filter_Str_Value.clear();
                        if(filter_Type != roachpb::In){
                            filter_Str_Value = col_filter_string_[count_string];
                            count_string++;
                        }else{
                            filter_Str_Values = col_String_values_[count_string_values];
                            count_string_values++;
                        }
                        if(!decode_Values_[col_id].empty()){
                            DecodeBytesValue(decode_Values_[col_id], &col_Str_Value);
                            col_values_filter->add_values(col_Str_Value);
                            null_values_filter->add_index(false);
                        }else{
                            col_values_filter->add_values(col_Str_Value);
                            null_values_filter->add_index(true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Str_Value,filter_Str_Values);
                            case roachpb::FilterType::StringContains:
                                return StringContains(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringEnds:
                                return StringEnds(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringStarts:
                                return StringStarts(col_Str_Value,filter_Str_Value);
                            default:
                                return true;
                        }
                    case ColumnType_DECIMAL:{
                        if(filter_Type != roachpb::In){
                            filter_Decimal_Value.Coeff.abs.clear();
                            filter_Decimal_Value = col_filter_decimal_[count_decimal];
                            count_decimal++;
                        }else{
                            filter_Decimal_Values = col_Decimal_values_[count_decimal_values];
                            count_decimal_values++;
                        }
                        col_Decimal_Value.Coeff.abs.clear();
                        col_Decimal_Value.Negative = false;
                        if(!decode_Values_[col_id].empty()){
                            DecodeDecimalValue(decode_Values_[col_id],&col_Decimal_Value);
                            roachpb::Decimal *dec_Value1 = col_values_filter->add_v_decimal();
                            dec_Value1->set_form(col_Decimal_Value._Form);
                            dec_Value1->set_negative(col_Decimal_Value.Negative);
                            dec_Value1->set_exponent(col_Decimal_Value.Exponent);
                            dec_Value1->set_neg(col_Decimal_Value.Coeff.neg);
                            if(col_Decimal_Value.Coeff.abs.size() != 0){
                                dec_Value1->add_abs(col_Decimal_Value.Coeff.abs[0]);
                            }
                            null_values_filter->add_index(false);
                        }else{
                            roachpb::Decimal *dec_Value2 = col_values_filter->add_v_decimal();
                            dec_Value2->set_form(0);
                            dec_Value2->set_negative(0);
                            dec_Value2->set_exponent(0);
                            dec_Value2->set_neg(0);
                            dec_Value2->add_abs(0);
                            null_values_filter->add_index(true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) == 0){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::Greater:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) == 1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::GreaterOrEqual:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) != -1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::Less:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) == -1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::LessOrEqual:{
                                if(CompareDecimals(col_Decimal_Value,filter_Decimal_Value) != 1){
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                            case roachpb::FilterType::In:{
                                for(int i=0;i<filter_Decimal_Values.size();i++){
                                    struct Decimal val = filter_Decimal_Values[i];
                                    if(CompareDecimals(col_Decimal_Value,val) == 0){
                                        return true;
                                    }
                                }
                                return false;
                            }
                            default:
                                return true;
                        }
                    }
                    case ColumnType_FLOAT:
                        if(filter_Type != roachpb::In){
                            filter_Double_Value = col_filter_double_[count_double];
                            count_double++;
                        }else{
                            filter_Double_Values = col_Double_values_[count_double_values];
                            count_double_values++;
                        }
                        if(!decode_Values_[col_id].empty()){
                            DecodeFloatValue(decode_Values_[col_id], &col_Double_Value);
                            col_values_filter->add_v_double(col_Double_Value);
                            null_values_filter->add_index(false);
                        }else{
                            col_values_filter->add_v_double(col_Double_Value);
                            null_values_filter->add_index(true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type) {
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Double_Value, filter_Double_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Double_Value, filter_Double_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Double_Value, filter_Double_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Double_Value, filter_Double_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Double_Value, filter_Double_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Double_Value, filter_Double_Values);
                            default:
                                return true;
                        }
                    case ColumnType_DATE:
                        if(filter_Type != roachpb::In){
                            filter_Int_Value = col_filter_int64_[count_int64];
                            count_int64++;
                        }else{
                            filter_Int_Values = col_Int64_values_[count_int64_values];
                            count_int64_values++;
                        }
                        if(!decode_Values_[col_id].empty()){
                            DecodeIntValue(decode_Values_[col_id], &col_Int_Value);
                            col_values_filter->add_v_int32(col_Int_Value);
                            null_values_filter->add_index(false);
                        }else{
                            col_values_filter->add_v_int32(col_Int_Value);
                            null_values_filter->add_index(true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Int_Value,filter_Int_Values);
                            default:
                                return true;
                        }
                    case ColumnType_TIMESTAMP:
                        if(filter_Type != roachpb::In){
                            filter_Int_Value = col_filter_int64_[count_int64];
                            count_int64++;
                        }else{
                            filter_Int_Values = col_Int64_values_[count_int64_values];
                            count_int64_values++;
                        }
                        if(!decode_Values_[col_id].empty()){
                            DecodeTimeValue(decode_Values_[col_id],&col_Time_Value);
                            col_Int_Value = col_Time_Value.t_timespec.tv_sec+col_Time_Value.t_timespec.tv_nsec/1e9;
                            col_values_filter->add_v_int(col_Int_Value);
                            null_values_filter->add_index( false);
                        }else{
                            col_values_filter->add_v_int(col_Int_Value);
                            null_values_filter->add_index( true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Int_Value,filter_Int_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Int_Value,filter_Int_Values);
                            default:
                                return true;
                        }
                    case ColumnType_UUID:
                        col_Str_Value.clear();
                        filter_Str_Value.clear();
                        if(filter_Type != roachpb::In){
                            filter_Str_Value = col_filter_string_[count_string];
                            count_string++;
                        }else{
                            filter_Str_Values = col_String_values_[count_string_values];
                            count_string_values++;
                        }
                        if(!decode_Values_[col_id].empty()){
                            Uuid *res = (Uuid *)malloc(Uuid_Size * sizeof(Uuid));
                            memset(res,0,Uuid_Size * sizeof(Uuid));
                            DecodeUUIDValue(decode_Values_[col_id], res);
                            col_Str_Value = convUUIDToString(res);    //UUID转成string
                            free(res);
                            col_values_filter->add_values(col_Str_Value);
                            null_values_filter->add_index(false);
                        }else{
                            col_values_filter->add_values(col_Str_Value);
                            null_values_filter->add_index(true);
                            switch (filter_Type){
                                case roachpb::IsNull:
                                    return true;
                                default:
                                    return false;
                            }
                        }
                        switch (filter_Type){
                            case roachpb::IsNotNull:
                                return true;
                            case roachpb::IsNull:
                                return false;
                            case roachpb::FilterType::Equal:
                                return equalto_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Greater:
                                return greater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::GreaterOrEqual:
                                return equOrGreater_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::Less:
                                return less_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::LessOrEqual:
                                return equOrLess_Function(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::In:
                                return In_Function(col_Str_Value,filter_Str_Values);
                            case roachpb::FilterType::StringContains:
                                return StringContains(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringEnds:
                                return StringEnds(col_Str_Value,filter_Str_Value);
                            case roachpb::FilterType::StringStarts:
                                return StringStarts(col_Str_Value,filter_Str_Value);
                            default:
                                return true;
                        }
                }
            }
        }


        bool addAndAdvance(const rocksdb::Slice& value) {
            // Don't include deleted versions (value.size() == 0), unless we've been
            // instructed to include tombstones in the results.
            if (value.size() > 0 || tombstones_) {
                rocksdb::Slice value_input = value;
                rocksdb::Slice key_input = cur_key_;//因为在解码时候会对cur_key_进行更改，直接使用cur_key_会影响endkey
                uint64_t tbl_id = 0;
                if(!DecodeTableIDKey(key_input,&tbl_id)) {
                    return setStatus(FmtStatus("unable to decode TableIDKey "));
                }
                uint64_t index_ID = 0;
                DecodeIndexIDKey(key_input,&index_ID);
                if(index_ID > 1){//过滤掉二级索引生成的KV
                    return advanceKey();
                }
                //没有filter条件时候获取行数count
                if(push_expr.required_cols_size() == 0 && push_expr.filters_size() == 0){
                    rs_count++;
                    vecResults.set_count(rs_count);
                    if (vecResults.count() == max_keys_) {
                        return false;
                    }
                    return advanceKey();
                }
                key_input.remove_suffix(1);
                keys_.clear();
                decode_Values_.clear();
                DecodeTableKey(key_input, push_expr, &keys_);
                DecodeSumRowReq(value_input,&decode_Values_,value_DecodeID_vec_);
                //处理filter过滤条件的操作
                if(push_expr.filters_size() != 0){
                    count_int64 = 0;
                    count_string = 0;
                    count_double = 0;
                    count_decimal = 0;
                    count_int64_values = 0;
                    count_string_values = 0;
                    count_double_values = 0;
                    count_decimal_values = 0;
                    filterID_used_.clear();
                    for(int i = 0;i < push_expr.filters_size();i++){
                        znbase::roachpb::FilterUnion *filter_Union = push_expr.mutable_filters(i);
                        PreOrderTraverse(filter_Union,&stack_res);//后序遍历
                        bool res = true;
                        if(!stack_res.empty()){
                            res = stack_res.top();
                            stack_res.pop();
                        }
                        if(!res){
                            clearValue();
                            return advanceKey();
                        }
                    }
                }
                if(push_expr.required_cols_size() == 0){
                    rs_count++;
                    vecResults.set_count(rs_count);
                    if (vecResults.count() == max_keys_) {
                        return false;
                    }
                    return advanceKey();
                }
                int direct_Flag = 0;
                bool col_Bool_Val;
                int64_t col_Int_Val;
                std::string col_Str_Val;
                double col_Double_Val;
                struct Decimal col_Decimal_Val;
                struct Time col_Time_Val ;
                for (int i = 0; i < push_expr.required_cols_size(); i++) {
                    znbase::roachpb::VecValue *col_values = vecResults.mutable_col_values(i);
                    znbase::roachpb::NullValue *null_values = vecResults.mutable_null_values(i);
                    auto col_id = col_values->col_id();
                    auto col_type = col_values->col_type();
                    if(std::find(filterID_used_.begin(),filterID_used_.end(),col_id) != filterID_used_.end()){//如果filterID_used_中有require需要的列 无需再次解码 直接赋值即可
                        int index = mapFilter_[col_id];
                        znbase::roachpb::VecValue *col_values_filter = vecResults_filter.mutable_col_values(index);
                        znbase::roachpb::NullValue *null_values_filter = vecResults_filter.mutable_null_values(index);
                        switch (col_type) {
                            case ColumnType_BOOL:
                                col_values->add_v_int32(col_values_filter->v_int32(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_INT:
                                col_values->add_v_int(col_values_filter->v_int(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_DATE:
                                col_values->add_v_int32(col_values_filter->v_int32(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                                col_values->add_values(col_values_filter->values(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_FLOAT:
                                col_values->add_v_double(col_values_filter->v_double(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_DECIMAL:{
                                roachpb::Decimal dec_filter_Value = col_values_filter->v_decimal(0);
                                roachpb::Decimal *val = col_values->add_v_decimal();
                                val->set_form(dec_filter_Value.form());
                                val->set_negative(dec_filter_Value.negative());
                                val->set_exponent(dec_filter_Value.exponent());
                                val->set_neg(dec_filter_Value.neg());
                                if(dec_filter_Value.abs().size() != 0){
                                    val->add_abs(dec_filter_Value.abs(0));
                                }
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            }
                            case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ:
                                col_values->add_v_int(col_values_filter->v_int(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_TIME:
                                col_values->add_v_int(col_values_filter->v_int(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                            case ColumnType_UUID:
                                col_values->add_values(col_values_filter->values(0));
                                null_values->add_index(null_values_filter->index(0));
                                break;
                        }
                    }else {
                        if(std::find(primaryID_vec_.begin(),primaryID_vec_.end(),col_id) != primaryID_vec_.end()){
                            direct_Flag = mapPrimary_[col_id];
                            switch (col_type) {
                                case ColumnType_BOOL:
                                    if(direct_Flag == 1){
                                        DecodeVarintDescending(keys_[col_id], &col_Int_Val);
                                    } else{
                                        DecodeVarintAscending(keys_[col_id], &col_Int_Val);
                                    }
                                    if(col_Int_Val){
                                        col_values->add_v_int32(1);
                                        null_values->add_index(false);
                                    } else{
                                        col_values->add_v_int32(0);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_INT:
                                    if(direct_Flag == 1){
                                        DecodeVarintDescending(keys_[col_id], &col_Int_Val);
                                    } else{
                                        DecodeVarintAscending(keys_[col_id], &col_Int_Val);
                                    }
                                    col_values->add_v_int(col_Int_Val);
                                    null_values->add_index(false);
                                    break;
                                case ColumnType_DATE:
                                    if(direct_Flag == 1){
                                        DecodeVarintDescending(keys_[col_id], &col_Int_Val);
                                    } else{
                                        DecodeVarintAscending(keys_[col_id], &col_Int_Val);
                                    }
                                    col_values->add_v_int32(col_Int_Val);
                                    null_values->add_index(false);
                                    break;
                                case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                                    col_Str_Val.clear();
                                    if(direct_Flag == 1){
                                        DecodeUnsafeStringDescending(keys_[col_id],&col_Str_Val);
                                    } else{
                                        DecodeUnsafeStringAscending(keys_[col_id],&col_Str_Val);
                                    }
                                    col_values->add_values(col_Str_Val);
                                    null_values->add_index(false);
                                    break;
                                case ColumnType_FLOAT:
                                    if(direct_Flag == 1){
                                        DecodeFloatDescending(keys_[col_id], &col_Double_Val);
                                    } else{
                                        DecodeFloatAscending(keys_[col_id], &col_Double_Val);
                                    }
                                    col_values->add_v_double(col_Double_Val);
                                    null_values->add_index(false);
                                    break;
                                case ColumnType_DECIMAL:{
                                    col_Decimal_Val.Coeff.abs.clear();
                                    col_Decimal_Val.Negative = false;
                                    if(direct_Flag == 1){
                                        DecodeDecimalDescending(keys_[col_id],&col_Decimal_Val);
                                    } else{
                                        DecodeDecimalAscending(keys_[col_id],&col_Decimal_Val);
                                    }
                                    roachpb::Decimal *dec_Value = col_values->add_v_decimal();
                                    dec_Value->set_form(col_Decimal_Val._Form);
                                    dec_Value->set_negative(col_Decimal_Val.Negative);
                                    dec_Value->set_exponent(col_Decimal_Val.Exponent);
                                    dec_Value->set_neg(col_Decimal_Val.Coeff.neg);
                                    if(col_Decimal_Val.Coeff.abs.size() != 0){
                                        dec_Value->add_abs(col_Decimal_Val.Coeff.abs[0]);
                                    }
                                    null_values->add_index(false);
                                    break;
                                }
                                case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ:
                                    if(direct_Flag == 1){
                                        DecodeTimeDescending(keys_[col_id],&col_Time_Val);
                                    } else{
                                        DecodeTimeAscending(keys_[col_id],&col_Time_Val);
                                    }
                                    col_Int_Val = col_Time_Val.t_timespec.tv_sec+col_Time_Val.t_timespec.tv_nsec/1e9;
                                    col_values->add_v_int(col_Int_Val);
                                    null_values->add_index(false);
                                    break;
                                case ColumnType_TIME://解码出来的int是微妙 转成秒写入 add_v_int
                                    if(direct_Flag == 1){
                                        DecodeVarintDescending(keys_[col_id], &col_Int_Val);
                                    } else{
                                        DecodeVarintAscending(keys_[col_id], &col_Int_Val);
                                    }
                                    col_values->add_v_int(col_Int_Val/1e6);
                                    null_values->add_index(false);
                                    break;
                                case ColumnType_UUID:{
                                    col_Str_Val.clear();
                                    if(direct_Flag == 1){
                                        DecodeUnsafeStringDescending(keys_[col_id],&col_Str_Val);
                                    } else{
                                        DecodeUnsafeStringAscending(keys_[col_id],&col_Str_Val);
                                    }
                                    col_Str_Val = convUUIDToString((unsigned char *)col_Str_Val.c_str());//string转unsign char*
                                    col_values->add_values(col_Str_Val);
                                    null_values->add_index(false);
                                    break;
                                }
                                default:
                                    return setStatus(FmtStatus("Decode TableKey unable support this type "));
                            }
                        } else {
                            switch (col_type) {
                                case ColumnType_BOOL:
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_v_int32(0);
                                        null_values->add_index(true);
                                    }else{
                                        DecodeBoolValue(decode_Values_[col_id], &col_Bool_Val);
                                        if(col_Bool_Val){
                                            col_values->add_v_int32(1);
                                            null_values->add_index(false);
                                        } else{
                                            col_values->add_v_int32(0);
                                            null_values->add_index(false);
                                        }
                                    }
                                    break;
                                case ColumnType_INT:
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_v_int(0);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeIntValue(decode_Values_[col_id], &col_Int_Val);
                                        col_values->add_v_int(col_Int_Val);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_DATE:
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_v_int32(0);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeIntValue(decode_Values_[col_id], &col_Int_Val);
                                        col_values->add_v_int32(col_Int_Val);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_COLLATEDSTRING:
                                    col_Str_Val.clear();
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_values(col_Str_Val);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeBytesValue(decode_Values_[col_id], &col_Str_Val);
                                        col_values->add_values(col_Str_Val);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_FLOAT:
                                    col_Double_Val = 0;
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_v_double(col_Double_Val);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeFloatValue(decode_Values_[col_id], &col_Double_Val);
                                        col_values->add_v_double(col_Double_Val);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_DECIMAL:
                                    col_Decimal_Val.Coeff.abs.clear();
                                    col_Decimal_Val.Negative = false;
                                    if(decode_Values_[col_id].empty()){
                                        roachpb::Decimal *dec_Value1 = col_values->add_v_decimal();
                                        dec_Value1->set_form(0);
                                        dec_Value1->set_negative(0);
                                        dec_Value1->set_exponent(0);
                                        dec_Value1->set_neg(0);
                                        dec_Value1->add_abs(0);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeDecimalValue(decode_Values_[col_id],&col_Decimal_Val);
                                        roachpb::Decimal *dec_Value2 = col_values->add_v_decimal();
                                        dec_Value2->set_form(col_Decimal_Val._Form);
                                        dec_Value2->set_negative(col_Decimal_Val.Negative);
                                        dec_Value2->set_exponent(col_Decimal_Val.Exponent);
                                        dec_Value2->set_neg(col_Decimal_Val.Coeff.neg);
                                        if(col_Decimal_Val.Coeff.abs.size() != 0){
                                            dec_Value2->add_abs(col_Decimal_Val.Coeff.abs[0]);
                                        }
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ://解码出来的是秒和纳秒 都转成秒写入 add_v_int
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_v_int(0);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeTimeValue(decode_Values_[col_id],&col_Time_Val);
                                        col_Int_Val = col_Time_Val.t_timespec.tv_sec+col_Time_Val.t_timespec.tv_nsec/1e9;
                                        col_values->add_v_int(col_Int_Val);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_TIME://解码出来的int是微妙 转成秒写入 add_v_int
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_v_int(0);
                                        null_values->add_index(true);
                                    } else{
                                        DecodeIntValue(decode_Values_[col_id], &col_Int_Val);
                                        col_values->add_v_int(col_Int_Val/1e6);
                                        null_values->add_index(false);
                                    }
                                    break;
                                case ColumnType_UUID:
                                    col_Str_Val.clear();
                                    if(decode_Values_[col_id].empty()){
                                        col_values->add_values(col_Str_Val);
                                        null_values->add_index(true);
                                    } else{
                                        Uuid *res = (Uuid *)malloc(Uuid_Size * sizeof(Uuid));
                                        memset(res,0,Uuid_Size * sizeof(Uuid));
                                        DecodeUUIDValue(decode_Values_[col_id], res);
                                        col_Str_Val = convUUIDToString(res);    //UUID转成string
                                        free(res);
                                        col_values->add_values(col_Str_Val);
                                        null_values->add_index(false);
                                    }
                                    break;
                                default:
                                    return setStatus(FmtStatus("Decode TableValue unable support this type "));
                            }
                        }
                    }
                }
                if(push_expr.filters_size()!=0){
                    clearValue();
                }
                rs_count++;
                vecResults.set_count(rs_count);
                if (vecResults.count() == max_keys_) {
                    return false;
                }
            }
            return advanceKey();
        }

        // seekVersion advances the iterator to point to an MVCC version for
        // the specified key that is earlier than <ts_wall_time,
        // ts_logical>. Returns false if the iterator is exhausted or an
        // error occurs. On success, advances the iterator to the next key.
        //
        // If the iterator is exhausted in the process or an error occurs,
        // return false, and true otherwise. If check_uncertainty is true,
        // then observing any version of the desired key with a timestamp
        // larger than our read timestamp results in an uncertainty error.
        //
        // TODO(peter): Passing check_uncertainty as a boolean is a bit
        // ungainly because it makes the subsequent comparison with
        // timestamp_ a bit subtle. Consider passing a
        // uncertainAboveTimestamp parameter. Or better, templatize this
        // method and pass a "check" functor.
        bool seekVersion(DBTimestamp desired_timestamp, bool check_uncertainty) {
            key_buf_.assign(cur_key_.data(), cur_key_.size());

            for (int i = 0; i < iters_before_seek_; ++i) {
                if (!iterNext()) {
                    return advanceKeyAtEnd();
                }
                if (cur_key_ != key_buf_) {
                    iters_before_seek_ = std::min<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
                    return advanceKeyAtNewKey(key_buf_);
                }
                if (desired_timestamp >= cur_timestamp_) {
                    iters_before_seek_ = std::min<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
                    if (check_uncertainty && timestamp_ < cur_timestamp_) {
                        return uncertaintyError(cur_timestamp_);
                    }
                    return addAndAdvance(cur_value_);
                }
            }

            iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
            if (!iterSeek(EncodeKey(key_buf_, desired_timestamp.wall_time, desired_timestamp.logical))) {
                return advanceKeyAtEnd();
            }
            if (cur_key_ != key_buf_) {
                return advanceKeyAtNewKey(key_buf_);
            }
            if (desired_timestamp >= cur_timestamp_) {
                if (check_uncertainty && timestamp_ < cur_timestamp_) {
                    return uncertaintyError(cur_timestamp_);
                }
                return addAndAdvance(cur_value_);
            }
            return advanceKey();
        }

        bool updateCurrent() {
            if (!iter_rep_->Valid()) {
                return false;
            }
            cur_raw_key_ = iter_rep_->key();
            cur_value_ = iter_rep_->value();
            cur_timestamp_ = kZeroTimestamp;
            if (!DecodeKey(cur_raw_key_, &cur_key_, &cur_timestamp_)) {
                return setStatus(FmtStatus("failed to split mvcc key"));
            }
            return true;
        }

        // iterSeek positions the iterator at the first key that is greater
        // than or equal to key.
        bool iterSeek(const rocksdb::Slice& key) {
            clearPeeked();
            iter_rep_->Seek(key);
            return updateCurrent();
        }

        // iterSeekReverse positions the iterator at the last key that is
        // less than key.
        bool iterSeekReverse(const rocksdb::Slice& key) {
            clearPeeked();

            // `SeekForPrev` positions the iterator at the last key that is less than or
            // equal to `key` AND strictly less than `ReadOptions::iterate_upper_bound`.
            iter_rep_->SeekForPrev(key);
            if (iter_rep_->Valid() && key.compare(iter_rep_->key()) == 0) {
                iter_rep_->Prev();
            }
            if (!updateCurrent()) {
                return false;
            }
            if (cur_timestamp_ == kZeroTimestamp) {
                // We landed on an intent or inline value.
                return true;
            }

            // We landed on a versioned value, we need to back up to find the
            // latest version.
            return backwardLatestVersion(cur_key_, 0);
        }

        bool iterNext() {
            if (reverse && peeked_) {
                // If we had peeked at the previous entry, we need to advance
                // the iterator twice to get to the real next entry.
                peeked_ = false;
                if (!iter_rep_->Valid()) {
                    // We were peeked off the beginning of iteration. Seek to the
                    // first entry, and then advance one step.
                    iter_rep_->SeekToFirst();
                    if (iter_rep_->Valid()) {
                        iter_rep_->Next();
                    }
                    return updateCurrent();
                }
                iter_rep_->Next();
                if (!iter_rep_->Valid()) {
                    return false;
                }
            }
            iter_rep_->Next();
            return updateCurrent();
        }

        bool iterPrev() {
            if (peeked_) {
                peeked_ = false;
                return updateCurrent();
            }
            iter_rep_->Prev();
            return updateCurrent();
        }

        // iterPeekPrev "peeks" at the previous key before the current
        // iterator position.
        bool iterPeekPrev(rocksdb::Slice* peeked_key) {
            if (!peeked_) {
                peeked_ = true;
                // We need to save a copy of the current iterator key and value
                // and adjust cur_raw_key_, cur_key and cur_value to point to
                // this saved data. We use a single buffer for this purpose:
                // saved_buf_.
                saved_buf_.resize(0);
                saved_buf_.reserve(cur_raw_key_.size() + cur_value_.size());
                saved_buf_.append(cur_raw_key_.data(), cur_raw_key_.size());
                saved_buf_.append(cur_value_.data(), cur_value_.size());
                cur_raw_key_ = rocksdb::Slice(saved_buf_.data(), cur_raw_key_.size());
                cur_value_ = rocksdb::Slice(saved_buf_.data() + cur_raw_key_.size(), cur_value_.size());
                rocksdb::Slice dummy_timestamp;
                if (!SplitKey(cur_raw_key_, &cur_key_, &dummy_timestamp)) {
                    return setStatus(FmtStatus("failed to split mvcc key"));
                }

                // With the current iterator state saved we can move the
                // iterator to the previous entry.
                iter_rep_->Prev();
            }

            if (!iter_rep_->Valid()) {
                // The iterator is now invalid, but note that this case is handled in
                // both iterNext and iterPrev. In the former case, we'll position the
                // iterator at the first entry, and in the latter iteration will be done.
                *peeked_key = rocksdb::Slice();
                return false;
            }

            rocksdb::Slice dummy_timestamp;
            if (!SplitKey(iter_rep_->key(), peeked_key, &dummy_timestamp)) {
                return setStatus(FmtStatus("failed to split mvcc key"));
            }
            return true;
        }

        // clearPeeked clears the peeked flag. This should be called before
        // any iterator movement operations on iter_rep_.
        void clearPeeked() {
            if (reverse) {
                peeked_ = false;
            }
        }

    public:
        DBIterator* const iter_;
        rocksdb::Iterator* const iter_rep_;
        const rocksdb::Slice start_key_;
        const rocksdb::Slice end_key_;
        const int64_t max_keys_;
        const DBTimestamp timestamp_;
        const rocksdb::Slice txn_id_;
        const uint32_t txn_epoch_;
        const int32_t txn_sequence_;
        const DBTimestamp txn_max_timestamp_;
        const DBIgnoredSeqNums txn_ignored_seqnums_;
        const bool inconsistent_;
        const bool tombstones_;
        const bool ignore_sequence_;
        const bool fail_on_more_recent_;
        const bool check_uncertainty_;

        //filter增加的
        std::vector<std::vector<std::string> > col_String_values_;
        std::vector<std::vector<int64_t> > col_Int64_values_;
        std::vector<std::vector<double> > col_Double_values_;
        std::vector<std::vector<struct Decimal> > col_Decimal_values_;

        std::vector<std::string> filter_Values;
        std::vector<int64_t> filter_Int_Values;
        std::vector<double > filter_Float_Values;
        std::vector<struct Decimal > filter_Decimal_Values;

        //逻辑过滤结果暂存调用栈
        std::stack<bool> stack_res;
        //过滤条件中的列ID与vecresult_filter的index映射表
        std::map<int, int> mapFilter_;
        //主键列ID和主键列ASC DESC列顺序的映射表
        std::map<int, int> mapPrimary_;
        //过滤条件中的列ID的集合
        std::vector<int> filterID_vec_;
        //已经过滤过了的列ID的集合
        std::vector<int> filterID_used_;

        //过滤条件中filter_value转成的值集合
        std::vector<int64_t> col_filter_int64_;
        std::vector<double > col_filter_double_;
        std::vector<std::string > col_filter_string_;
        std::vector<struct Decimal> col_filter_decimal_;
        //filter_value转成的值集合的位置
        int count_int64;
        int count_string;
        int count_double;
        int count_decimal;
        //filter_values转成的值集合的位置
        int count_int64_values;
        int count_string_values;
        int count_double_values;
        int count_decimal_values;

        DBVecResults & dbResults;
        znbase::roachpb::VecResults & vecResults;
        znbase::roachpb::VecResults & vecResults_filter;
        znbase::roachpb::PushDownExpr push_expr;
        std::vector<rocksdb::Slice> decode_Values_;
        int rs_count;
        std::vector<int> primaryID_vec_;
        std::vector<int> value_DecodeID_vec_;
        std::vector<rocksdb::Slice> keys_;
        std::unique_ptr<rocksdb::WriteBatch> intents_;
        // most_recent_timestamp_ stores the largest timestamp observed that is
        // above the scan timestamp. Only applicable if fail_on_more_recent_ is
        // true. If set and no other error is hit, a WriteToOld error will be
        // returned from the scan.
        DBTimestamp most_recent_timestamp_;
        std::string key_buf_;
        std::string saved_buf_;
        bool peeked_;
        znbase::storage::engine::enginepb::MVCCMetadata meta_;
        // cur_raw_key_ holds either iter_rep_->key() or the saved value of
        // iter_rep_->key() if we've peeked at the previous key (and peeked_
        // is true).
        rocksdb::Slice cur_raw_key_;
        // cur_key_ is the decoded MVCC key, separated from the timestamp
        // suffix.
        rocksdb::Slice cur_key_;
        // cur_value_ holds either iter_rep_->value() or the saved value of
        // iter_rep_->value() if we've peeked at the previous key (and
        // peeked_ is true).
        rocksdb::Slice cur_value_;
        // cur_timestamp_ is the timestamp for a decoded MVCC key.
        DBTimestamp cur_timestamp_;
        int iters_before_seek_;
    };

    typedef vecScanner<false> vecForwardScanner;
    typedef vecScanner<true> vecReverseScanner;

}  // namespace znbase