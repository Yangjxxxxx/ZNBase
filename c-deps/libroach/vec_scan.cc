/*
 * @author jiadx
 * @date 2021/4/7
 * @version 1.0
*/
#include <libroach.h>
#include <sstream>
#include "comparator.h"
#include "encoding.h"
#include "keys.h"
#include "protos/roachpb/api.pb.h"
#include "vec_scan_data.h"
#include "vec_scan.h"
#include "mvcc.h"

using namespace znbase;

int32_t getColType(DBPushdown &pushdown, int32_t col_id) {
    for (int i = 0; i < pushdown.col_count; i++) {
        if (col_id == pushdown.col_ids[i])
            return pushdown.col_types[i];
    }
    return -1;
}


DBVecResults VecScan(DBEngine *db, DBSlice start, DBSlice end, DBTimestamp timestamp,
                     DBSlice push_str,// roachpb.PushDownExpr的序列化 // TODO 从Go层构建序列化，传送到C层，解析不出数据
                     int64_t max_keys, DBTxn txn, bool inconsistent, bool reverse,
                     bool tombstones, bool ignore_sequence, bool fail_on_more_recent) {
    DBVecResults dbResults;
    roachpb::VecResults vecResults;
    roachpb::VecResults vecResults_filter_;
    vecscan_internal(db, start, end, timestamp, push_str,
                     max_keys, txn, inconsistent, reverse, tombstones, ignore_sequence, fail_on_more_recent,
                     dbResults, vecResults,vecResults_filter_);
    if (dbResults.status.len > 0) {
        return dbResults;
    }
//  std::cerr << "!!!!!!!!!C.vecResults.count = " << vecResults.count() << "\n";
    dbResults.data.len = vecResults.ByteSize();
    dbResults.data.data = static_cast<char *>(malloc(dbResults.data.len));
    vecResults.SerializeToArray(dbResults.data.data, dbResults.data.len);
    return dbResults;
}

namespace znbase {
    void vecscan_internal(DBEngine *db, DBSlice start, DBSlice end, DBTimestamp timestamp, roachpb::PushDownExpr pushdown,
                          int64_t max_keys, DBTxn txn, bool inconsistent, bool reverse, bool tombstones,
                          bool ignore_sequence, bool fail_on_more_recent,
                          DBVecResults &dbResults, roachpb::VecResults &vecResults,roachpb::VecResults &vecResults_filter) {
//    int64_t t_start = GetTimeNow();
        DBIterator *iter = NULL;
        try {
            DBIterOptions iter_opt = DBIterOptions{false, DBKey{start, 0, 0}, DBKey{end, 0, 0}};
            iter = DBNewIter(db, iter_opt);
            ScopedStats scoped_iter(iter);
            if (reverse) {
                vecReverseScanner scanner(iter, end, start, max_keys, timestamp, pushdown, txn, inconsistent, tombstones,
                                          ignore_sequence, fail_on_more_recent, dbResults, vecResults,vecResults_filter);
                scanner.scan();
            } else {
                vecForwardScanner scanner(iter, start, end, max_keys, timestamp, pushdown, txn, inconsistent, tombstones,
                                          ignore_sequence, fail_on_more_recent, dbResults, vecResults,vecResults_filter);
                scanner.scan();
            }
        }
        catch (std::string &ex) {
            dbResults.resume_key = DBSlice{NULL, 0};
            dbResults.status = ToDBString(ex);
        }
        if (iter != NULL) {
            DBIterDestroy(iter);
        }
//    std::cerr << "!!!!!!!!!C.VecScan : time=" << (GetTimeNow() - t_start) / 1e6 << " ms \n";
        //std::cerr << "VecScan2 :: " << ofDBSlice(start) << "  ***  " << ofDBSlice(end) << ",data_len="<<rs.data.len << "\n";
        return;
    }

    void vecscan_internal(DBEngine *db, DBSlice start, DBSlice end, DBTimestamp timestamp,
                          DBSlice push_str,
                          int64_t max_keys, DBTxn txn, bool inconsistent, bool reverse,
                          bool tombstones, bool ignore_sequence, bool fail_on_more_recent,
                          DBVecResults &dbResults, roachpb::VecResults &vecResults,roachpb::VecResults &vecResults_filter) {
        // 解析下推表达式
        znbase::roachpb::PushDownExpr push_expr = {};
        if (!push_expr.ParseFromArray(push_str.data, push_str.len)) {
            std::string msg = "parse pushdown error.";
            dbResults.status = ToDBString(msg);
        }
        vecscan_internal(db, start, end, timestamp, push_expr, max_keys, txn, inconsistent, reverse, tombstones,
                         ignore_sequence, fail_on_more_recent, dbResults, vecResults,vecResults_filter);

        return;
    }
}