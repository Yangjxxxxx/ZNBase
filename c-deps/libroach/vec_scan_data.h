/*
 * @author jiadx
 * @date 2021/4/7
 * @version 1.0
*/
#pragma once

#include <libroach.h>
#include "mvcc.h"
#include "comparator.h"
#include "encoding.h"
#include "keys.h"
#include "protos/roachpb/api.pb.h"
#include <stack>

// 模拟customer表数据
using namespace znbase;

typedef struct {
    int64_t custkey;
    std::string name;
    std::string address;
    int64_t nationkey;
    std::string phone;
    float acctbal;//DECIMAL
    std::string mktsegment;
    std::string comment;
    int64_t rowid;
} Customer;

const int CustomerTypes[] = {ColumnType_INT, ColumnType_STRING, ColumnType_STRING, ColumnType_INT, ColumnType_STRING, ColumnType_DECIMAL, ColumnType_STRING, ColumnType_STRING,ColumnType_INT};

const int CustomerDecTypes[] = {ColumnType_INT, ColumnType_STRING, ColumnType_STRING, ColumnType_INT, ColumnType_STRING, ColumnType_DECIMAL, ColumnType_STRING, ColumnType_STRING};


const int CustomerTypes2[] = {ColumnType_INT, ColumnType_STRING, ColumnType_INT, ColumnType_STRING};

const int CustomerTypes3[] = {ColumnType_INT, ColumnType_INT,ColumnType_INT,ColumnType_INT,ColumnType_DECIMAL, ColumnType_DATE,ColumnType_STRING,ColumnType_FLOAT};

const int CustomerTypes4[] = {ColumnType_INT, ColumnType_INT,ColumnType_INT,ColumnType_INT};

const int CustomerTypes6[] = {ColumnType_INT, ColumnType_UUID};

const int CustomerTypes7[] = {ColumnType_UUID, ColumnType_INT};

const int CustomerTypes9[] = {ColumnType_INT, ColumnType_STRING,ColumnType_STRING,ColumnType_STRING,ColumnType_INT,ColumnType_INT,ColumnType_DATE};

const Customer CustomerData[] = {
        Customer{1, "Customer#000000001", "IVhzIApeRb ot,c,E", 15, "25-989-741-2988", 0.071156, "BUILDING",
                 "	to the even, regular platelets. regular, ironic epitaphs nag e", 1},
        Customer{2, "Customer#000000002", "XSTf4,NCwDVaWNe6tEgvwfmRchLXak", 13, "23-768-687-3665", 0.012165, "BUILDING",
                 "	l accounts. blithely ironic theodolites integrate boldly: caref", 1},
        Customer{3, "Customer#000000003", "MG9kdTD2WBHm", 15, "11-719-748-3364", 0.749812, "AUTOMOBILE",
                 "	 deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov",
                 1},
        Customer{4, "Customer#000000004", "XxVSJsLAGtn", 4, "14-128-190-5944", 0.286683, "BUILDING",
                 "	 requests. final, regular ideas sleep final accou", 1},
        Customer{5, "Customer#000000005", "KvpyuHCplrB84WgAiGV6sYpZq7Tj", 15, "13-750-942-6364", 0.079447, "BUILDING",
                 "	n accounts will have to unwind. foxes cajole accor", 1},
        Customer{6, "Customer#000000006", "sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn", 13, "30-114-968-4951", 0.763857,
                 "BUILDING",
                 "	tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious",
                 1},
};

void preBuildFilter(znbase::roachpb::FilterUnion *filter_input,std::stack<znbase::roachpb::FilterType> &filter_Type_vec,std::stack<std::string> &filter_ID_vec,std::stack<std::string> &filter_Value_vec);

void PreOrderTest(znbase::roachpb::FilterUnion *filter_union);

roachpb::PushDownExpr buildCustomerSchema();

roachpb::PushDownExpr buildLineitemSchema();

roachpb::PushDownExpr buildPushSchema2();

roachpb::PushDownExpr buildPushSchema3();

roachpb::PushDownExpr buildPushSchema4();

roachpb::PushDownExpr buildPushSchema6();

roachpb::PushDownExpr buildPushSchema7();

roachpb::PushDownExpr buildHeroSchema();

roachpb::PushDownExpr buildCustomerDecSchema();