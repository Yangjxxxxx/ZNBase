/*
 * @author jiadx
 * @date 2021/4/7
 * @version 1.0
*/
#include "db.h"
#include <libroach.h>
#include <roachpb/api.pb.h>
#include "testutils.h"
#include "vec_scan_data.h"

using namespace znbase;
using namespace testutils;

//测试过滤条件的构建功能
/*
 * Hero表Schema信息:
 *
 */
TEST(VecScan, preBuildFilter) {
    roachpb::PushDownExpr push_expr;
    int col_count = 7;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes9[i]);//hero表
    }
    push_expr.add_primary_cols(1);
    std::stack<znbase::roachpb::FilterType> filter_type_vec;
    filter_type_vec.push(roachpb::FilterType::Less);
    filter_type_vec.push(roachpb::FilterType::LessOrEqual);
    filter_type_vec.push(roachpb::FilterType::Or);
    filter_type_vec.push(roachpb::FilterType::Greater);
    filter_type_vec.push(roachpb::FilterType::GreaterOrEqual);
    filter_type_vec.push(roachpb::FilterType::Or);
    filter_type_vec.push(roachpb::FilterType::Or);
    std::stack<std::string> filter_ID_vec;
    filter_ID_vec.push("2");
    filter_ID_vec.push("3");
    filter_ID_vec.push("1");
    filter_ID_vec.push("4");
    std::stack<std::string> filter_Value_vec;
    filter_Value_vec.push("100");
    filter_Value_vec.push("100");
    filter_Value_vec.push("20");
    filter_Value_vec.push("30");

    znbase::roachpb::FilterUnion *filter_Union = push_expr.add_filters();
    preBuildFilter(filter_Union,filter_type_vec,filter_ID_vec,filter_Value_vec);
    for(int i = 0;i < push_expr.filters_size();i++){
        znbase::roachpb::FilterUnion *filter_union = push_expr.mutable_filters(i);
        PreOrderTest(filter_union);//后序遍历
    }
}


TEST(VecScan, Decimal) {
    // Use a real directory, we need to create a file_registry.
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();

    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice("/home/weili/newsql/gowork/znbase100"), db_opts).data, NULL);//数据库测试TPCH功能 启动数据库--store的路径
    roachpb::PushDownExpr push_expr = buildCustomerDecSchema();
    push_expr.add_required_cols(6);
    auto push_str = push_expr.SerializeAsString();

    unsigned char start_char[] = {194};//TableID 不同数据库的表的TableID不同
    unsigned char end_char[] = {195};
    DBSlice start = DBSlice{(char *) start_char, 1};
    DBSlice end = DBSlice{(char *) end_char, 1};
    int max_keys = 1e5;
    DBTimestamp ts{GetTimeNow(), 0};
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), max_keys, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));

    std::cout << "VecValue count == "<< rs.count() <<std::endl;
    for (int i = 0; i < rs.col_ids_size(); i++) {
        znbase::roachpb::VecValue *col_values = rs.mutable_col_values(i);
        if(col_values->v_decimal_size() != 0){
            for(int j=0;j<col_values->v_decimal_size();j++){
                roachpb::Decimal *dec_Value = col_values->mutable_v_decimal(j);
                std::cout <<"dec_Value->form()== " <<   dec_Value->form() << "  dec_Value->exponent() == "<<dec_Value->exponent() << "   dec_Value->negative() == "<<dec_Value->negative()  << std::endl;
                std::cout <<"dec_Value->abs() == " <<   dec_Value->abs(0) <<std::endl;
                std::cout <<"dec_Value->neg() == " <<   dec_Value->neg() <<std::endl;
            }
        }
    }
    DBClose(db);
}


TEST(VecScan, filter) {
    // Use a real directory, we need to create a file_registry.
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();

    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice("/data/znbase"), db_opts).data, NULL);//数据库测试TPCH功能 启动数据库--store的路径
    roachpb::PushDownExpr push_expr = buildHeroSchema();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);
    push_expr.add_required_cols(3);
    push_expr.add_required_cols(4);
    auto push_str = push_expr.SerializeAsString();

    unsigned char start_char[] = {198};//TableID 不同数据库的表的TableID不同
    unsigned char end_char[] = {199};
    DBSlice start = DBSlice{(char *) start_char, 1};
    DBSlice end = DBSlice{(char *) end_char, 1};
    int max_keys = 1e5;
    DBTimestamp ts{GetTimeNow(), 0};
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), max_keys, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));

    std::cout << "VecValue count == "<< rs.count() <<std::endl;
    for (int i = 0; i < rs.col_ids_size(); i++) {
        znbase::roachpb::VecValue *col_values = rs.mutable_col_values(i);
        for (int j = 0; j < rs.count(); j++) {
            if (col_values->values_size() != 0) {
                std::cout << "第" << col_values->col_id() << "列,第" << j + 1 << "行的值为" << col_values->values(j) << std::endl;
            }
            if (col_values->v_int_size() != 0) {
                std::cout << "第" << col_values->col_id() << "列,第" << j + 1 << "行的值为" << col_values->v_int(j) << std::endl;
            }
            if (col_values->v_double_size() != 0) {
                std::cout << "第" << col_values->col_id() << "列,第" << j + 1 << "行的值为" << col_values->v_double(j) << std::endl;
            }
            if (col_values->v_float_size() != 0) {
                std::cout << "第" << col_values->col_id() << "列,第" << j + 1 << "行的值为" << col_values->v_float(j) << std::endl;
            }
        }
    }
    DBClose(db);
}

int scan_all(DBEngine *db, DBSlice start, DBSlice end, std::string push_str, bool internal, int max_keys) {
    std::string start_str = std::string{start.data, size_t(start.len)};
    DBTimestamp ts{GetTimeNow(), 0};
    int count = 0;
    bool scan_end = false;
    while (!scan_end) {
        DBSlice start_key = ToDBSlice(start_str);
        //std::cerr << "start_key : " << sliceToString(start_key) << " , end_key : " << sliceToString(end) << "\n";
        DBVecResults results;
        znbase::roachpb::VecResults rs;
        znbase::roachpb::VecResults rs_filter;
        if (internal) {
            vecscan_internal(db, start_key, end, ts, ToDBSlice(push_str), max_keys, DBTxn{}, false, false, false, false,
                             false, results, rs,rs_filter);
        } else {
            results = VecScan(db, start_key, end, ts, ToDBSlice(push_str), max_keys, DBTxn{}, false, false, false, false,
                              false);
            if (results.data.len > 0) {
                rs.ParseFromArray(results.data.data, results.data.len);
                delete results.data.data;
            }
        }
        start_str = ToString(results.resume_key);
        EXPECT_STREQ(results.status.data, NULL);
        if (results.status.data != NULL) {
            break;
        }
        if (rs.count() > 0) {
            // 输出第一行
            std::cerr << " first row : " << rs.col_values(0).v_int(0) << "\n";
        }
        //std::cerr << "resume_key : " << sliceToString(results.resume_key) << "\n";
        scan_end = results.resume_key.len == 0;

        count += rs.count();
    }
    return count;
}

TEST(VecScan, customer) {
    // Use a real directory, we need to create a file_registry.
    TempDirHandler dir;

    DBOptions db_opts = defaultDBOptions();

    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice("/data/znbase"), db_opts).data, NULL);//数据库测试TPCH功能 启动数据库--store的路径
    roachpb::PushDownExpr push_expr = buildCustomerSchema();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);
    push_expr.add_required_cols(4);
    push_expr.add_required_cols(6);
    auto push_str = push_expr.SerializeAsString();

    unsigned char start_char[] = {195};// TableID 不同数据库的表的TableID不同
    unsigned char end_char[] = {196};
    std::string start = std::string{(char *) start_char, 1};
    int max_keys = 1e5;

    int count = scan_all(db, DBSlice{(char *) start_char, 1}, DBSlice{(char *) end_char, 1}, push_str, false, max_keys);

    printf("Result Count : %d\n", count);
    DBClose(db);

}

// 测试protobuf编码的性能
TEST(VecScan, bench) {
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();

    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice("/data/znbase"), db_opts).data, NULL);//数据库测试TPCH功能 启动数据库--store的路径
    roachpb::PushDownExpr push_expr = buildLineitemSchema();
    push_expr.add_required_cols(3);// l_suppkey
    push_expr.add_required_cols(5);// l_quantity
    auto push_str = push_expr.SerializeAsString();

    unsigned char start_char[] = {197}; // TableID 不同数据库的表的TableID不同
    unsigned char end_char[] = {198};

    int max_keys = 1e6;
    auto start_t = GetTimeNow();
    for (int i = 0; i < 1; i++) {
        int count = scan_all(db, DBSlice{(char *) start_char, 1}, DBSlice{(char *) end_char, 1}, push_str, false, max_keys);

        printf("Result Count : %d\n", count);
    }
    // 11333.14 ms
    printf("****Protobuf time : %.2f ms\n", (GetTimeNow() - start_t) / 1e6);
    start_t = GetTimeNow();
    // 不进行protobuf序列化
    for (int i = 0; i < 1; i++) {
        int count = scan_all(db, DBSlice{(char *) start_char, 1}, DBSlice{(char *) end_char, 1}, push_str, true, max_keys);

        printf("Result Count : %d\n", count);
    }
    // 11089.90 ms
    printf("****NO Protobuf time : %.2f ms\n", (GetTimeNow() - start_t) / 1e6);

    // 2者差别不大，说明protobuf序列化性能还是很好的

    DBClose(db);

}

TEST(MVCCScan, MVCC) {//测试rocksdb的mvccscan读取
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();
    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);

    unsigned char data1_key[] = {190, 137, 18, 108, 109, 108, 0, 1, 161, 136};//  10
    unsigned char data1_value[] = {198, 157, 8, 185, 10, 19, 246, 1, 54, 4, 115, 99, 117, 116};//14
    unsigned char data2_key[] = {190, 137, 18, 106, 97, 99, 107, 0, 1, 161, 136};// 11
    unsigned char data2_value[] = {20, 73, 24, 164, 10, 19, 248, 1, 54, 6, 115, 99, 117, 49, 49, 116};//16
    //values(124,'tom',25,'niubi');.
    unsigned char data3_key[] = {190, 137, 18, 116, 111, 109, 0, 1, 161, 136};
    unsigned char data3_value[] = {255, 122, 93, 164, 10, 19, 248, 1, 54, 5, 110, 105, 117, 98, 105};
    //values(125,'fevir',28,'nght');
    unsigned char data4_key[] = {190, 137, 18, 102, 101, 118, 105, 114, 0, 1, 164, 136};
    unsigned char data4_value[] = {185, 245, 19, 84, 10, 19, 250, 1, 54, 4, 110, 103, 104, 116};
    // Put数据
    // KV数据
    DBPut(db, DBKey{DBSlice{(char *) data1_key, sizeof(data1_key)}, 100, 0},
          DBSlice{(char *) data1_value, sizeof(data1_value)});
    // 表数据(123,'lml',25,'scut')
    DBPut(db, DBKey{DBSlice{(char *) data2_key, sizeof(data2_key)}, 100, 0},
          DBSlice{(char *) data2_value, sizeof(data2_value)});
    DBPut(db, DBKey{DBSlice{(char *) data3_key, sizeof(data3_key)}, 100, 0},
          DBSlice{(char *) data3_value, sizeof(data3_value)});
    // 表数据(123,'lml',25,'scut')
    DBPut(db, DBKey{DBSlice{(char *) data4_key, sizeof(data4_key)}, 100, 0},
          DBSlice{(char *) data4_value, sizeof(data4_value)});

    unsigned char start_char[] = {190, 137};//TableID 不同数据库的表的TableID不同
    unsigned char end_char[] = {190, 138};
    DBSlice start = DBSlice{(char *) start_char, 2};
    DBSlice end = DBSlice{(char *) end_char, 2};
    DBTimestamp ts{10000, 0};
    DBIterOptions iter_opt = DBIterOptions{};
    DBIterator *iter = DBNewIter(db, iter_opt);
    DBScanResults results = MVCCScan(iter, start, end, ts, 100, DBTxn{}, false, false, false, false, false);
    ASSERT_EQ(results.data.count, 4);// 行数
}

TEST(VecScan, ManyPrimaryKey) {
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();
    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);

    std::vector<std::string> col_value2 = {"fevir","jack","lml","tom"};//第二列
    std::vector<std::string> col_value4 = {"nght","scu11t","scut","niubi"};//第四列
    std::vector<int> col_value1 = {125,124,123,124};//第一列
    std::vector<int> col_value3 = {28,25,25,25};//第三列

    //* 多列主键
    //* CREATE TABLE test11(id INT,name STRING , age INT, school STRING, INDEX idx (age ASC),CONSTRAINT "primary" PRIMARY KEY (name ASC, age ASC));
    //values(123,'lml',25,'scut')
    unsigned char data1_key[] = {190, 137, 18, 108, 109, 108, 0, 1, 161, 136};//  10
    unsigned char data1_value[] = {198, 157, 8, 185, 10, 19, 246, 1, 54, 4, 115, 99, 117, 116};//14
    //values(124,'jack',25,'scu11t');
    unsigned char data2_key[] = {190, 137, 18, 106, 97, 99, 107, 0, 1, 161, 136};// 11
    unsigned char data2_value[] = {20, 73, 24, 164, 10, 19, 248, 1, 54, 6, 115, 99, 117, 49, 49, 116};//16
    //values(124,'tom',25,'niubi');.
    unsigned char data3_key[] = {190, 137, 18, 116, 111, 109, 0, 1, 161, 136};
    unsigned char data3_value[] = {255, 122, 93, 164, 10, 19, 248, 1, 54, 5, 110, 105, 117, 98, 105};

    //values(125,'fevir',28,'nght');
    unsigned char data4_key[] = {190, 137, 18, 102, 101, 118, 105, 114, 0, 1, 164, 136};
    unsigned char data4_value[] = {185, 245, 19, 84, 10, 19, 250, 1, 54, 4, 110, 103, 104, 116};
    // Put数据
    // KV数据
    DBPut(db, DBKey{DBSlice{(char *) data1_key, sizeof(data1_key)}, 100, 0},
          DBSlice{(char *) data1_value, sizeof(data1_value)});
    // 表数据(123,'lml',25,'scut')
    DBPut(db, DBKey{DBSlice{(char *) data2_key, sizeof(data2_key)}, 100, 0},
          DBSlice{(char *) data2_value, sizeof(data2_value)});
    DBPut(db, DBKey{DBSlice{(char *) data3_key, sizeof(data3_key)}, 100, 0},
          DBSlice{(char *) data3_value, sizeof(data3_value)});
    // 表数据(123,'lml',25,'scut')
    DBPut(db, DBKey{DBSlice{(char *) data4_key, sizeof(data4_key)}, 100, 0},
          DBSlice{(char *) data4_value, sizeof(data4_value)});

    unsigned char start_char[] = {190, 137};
    unsigned char end_char[] = {190, 138};
    DBSlice start = DBSlice{(char *) start_char, 2};
    DBSlice end = DBSlice{(char *) end_char, 2};
    DBTimestamp ts{10000, 0};

    //表的schema信息
    roachpb::PushDownExpr push_expr = buildPushSchema2();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);
    push_expr.add_required_cols(3);
    push_expr.add_required_cols(4);

    auto push_str = push_expr.SerializeAsString();
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), 100, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));
    ASSERT_EQ(rs.count(), 4);// 行数
    ASSERT_EQ(rs.col_ids_size(), 4);// 需要返回的列数
    for (int i = 0; i < rs.col_ids_size(); i++) {
        znbase::roachpb::VecValue *col_values = rs.mutable_col_values(i);
        for (int j = 0; j < rs.count(); j++) {
            if (col_values->values_size() != 0) {
                if (col_values->col_id() == 2){
                    ASSERT_EQ(col_values->values(j),col_value2[j]);
                }else if(col_values->col_id() == 4){
                    ASSERT_EQ(col_values->values(j),col_value4[j]);
                }
            }
            if (col_values->v_int_size() != 0) {
                if (col_values->col_id() == 1){
                    ASSERT_EQ(col_values->v_int(j),col_value1[j]);
                }else if(col_values->col_id() == 3){
                    ASSERT_EQ(col_values->v_int(j),col_value3[j]);
                }
            }
        }
    }
}

TEST(VecScan, ManyColumnType) {
    /*
     测试多列混合类型的乱序主键
     */
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();
    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);

    //* 多列主键
    //* CREATE TABLE if not EXISTS lineitem (
    //                           l_orderkey bigint NOT NULL,
    //                           l_partkey bigint NOT NULL,
    //                           l_suppkey bigint NOT NULL,
    //                           l_linenumber bigint NOT NULL,
    //                           l_tax decimal(10,2) NOT NULL,
    //                           l_shipdate date NOT NULL,
    //                           l_shipinstruct char(25) DEFAULT NULL,
    //                           l_shipcost FLOAT NOT NULL,
    //                           PRIMARY KEY (l_shipinstruct,l_shipcost,l_orderkey, l_linenumber, l_shipdate, l_partkey,l_tax)
    //                         );
    //insert into lineitem values(112131,4215645,87741,369877,342.02,'2012-12-02','I dsdds gh',1234.348);
    unsigned char data1_key[] = {190, 137, 18, 73, 32, 100, 115, 100, 100, 115, 32, 103, 104, 0, 1, 5, 64, 147, 73, 100,
                                 90, 28, 172, 8, 248, 1, 182, 3, 248, 5, 164, 213, 247, 61, 60, 248, 64, 83, 93, 43, 7,
                                 85, 4, 0, 136};//  10
    unsigned char data1_value[] = {223, 0, 143, 184, 10, 51, 250, 218, 10};//14
    //insert into lineitem values(1125845,54785645,8741235158741,398897,111.02,'2000-08-02','I ha dream',12.3548);
    unsigned char data2_key[] = {190, 137, 18, 73, 32, 104, 97, 32, 100, 114, 101, 97, 109, 0, 1, 5, 64, 40, 181, 168, 88,
                                 121, 61, 217, 248, 17, 45, 213, 250, 8, 156, 165, 5, 113, 247, 43, 163, 249, 3, 67, 246,
                                 109, 43, 3, 23, 4, 0, 136};// 11
    unsigned char data2_value[] = {171, 1, 234, 104, 10, 51, 170, 187, 187, 163, 231, 252, 3};//16
    //insert into lineitem values(1141235,41234785645,88741,798897,145.02,'2008-08-12','asf dsream',1223.3898);
    unsigned char data3_key[] = {190, 137, 18, 97, 115, 102, 32, 100, 115, 114, 101, 97, 109, 0, 1, 5, 64, 147, 29, 143,
                                 39, 187, 47, 236, 248, 17, 105, 243, 248, 12, 48, 177, 247, 55, 23, 250, 9, 153, 200,
                                 229, 109, 43, 3, 91, 4, 0, 136};
    unsigned char data3_value[] = {51, 91, 73, 28, 10, 51, 202, 234, 10};

    //insert into lineitem values(113425,54731245,874121,36997,121.02,'2001-05-22','df ge f',1223.3788);
    unsigned char data4_key[] = {190, 137, 18, 100, 102, 32, 103, 101, 32, 102, 0, 1, 5, 64, 147, 29, 131, 228, 37, 174,
                                 230, 248, 1, 187, 17, 247, 144, 133, 247, 44, 200, 249, 3, 67, 33, 237, 43, 3, 43, 4, 0,
                                 136};
    unsigned char data4_value[] = {152, 172, 229, 46, 10, 51, 146, 218, 106};
    //insert into lineitem values(34545,547425,858741,3698777,1234.02,'2000-06-13','fadsg eam',2.3348);
    unsigned char data5_key[] = {190, 137, 18, 102, 97, 100, 115, 103, 32, 101, 97, 109, 0, 1, 5, 64, 2, 173, 171, 159,
                                 85, 155, 61, 247, 134, 241, 248, 56, 112, 89, 247, 43, 113, 248, 8, 90, 97, 43, 25, 69,
                                 4, 0, 136};
    unsigned char data5_value[] = {242, 9, 158, 187, 10, 51, 234, 233, 104};
    //insert into lineitem values(5842135,541285645,58741,36897,123.02,'2007-11-11','gsae dream',1.35);
    unsigned char data6_key[] = {190, 137, 18, 103, 115, 97, 101, 32, 100, 114, 101, 97, 109, 0, 1, 5, 63, 245, 153, 153,
                                 153, 153, 153, 154, 248, 89, 36, 215, 247, 144, 33, 247, 54, 4, 249, 32, 67, 93, 13, 43,
                                 3, 47, 4, 0, 136};
    unsigned char data6_value[] = {230, 172, 82, 21, 10, 51, 234, 149, 7};
    // Put数据
    // KV数据
    DBPut(db, DBKey{DBSlice{(char *) data1_key, sizeof(data1_key)}, 100, 0},
          DBSlice{(char *) data1_value, sizeof(data1_value)});

    DBPut(db, DBKey{DBSlice{(char *) data2_key, sizeof(data2_key)}, 100, 0},
          DBSlice{(char *) data2_value, sizeof(data2_value)});
    DBPut(db, DBKey{DBSlice{(char *) data3_key, sizeof(data3_key)}, 100, 0},
          DBSlice{(char *) data3_value, sizeof(data3_value)});

    DBPut(db, DBKey{DBSlice{(char *) data4_key, sizeof(data4_key)}, 100, 0},
          DBSlice{(char *) data4_value, sizeof(data4_value)});

    DBPut(db, DBKey{DBSlice{(char *) data5_key, sizeof(data5_key)}, 100, 0},
          DBSlice{(char *) data5_value, sizeof(data5_value)});

    DBPut(db, DBKey{DBSlice{(char *) data6_key, sizeof(data6_key)}, 100, 0},
          DBSlice{(char *) data6_value, sizeof(data6_value)});

    unsigned char start_char[] = {190, 137};
    unsigned char end_char[] = {190, 138};
    DBSlice start = DBSlice{(char *) start_char, 2};
    DBSlice end = DBSlice{(char *) end_char, 2};
    DBTimestamp ts{10000, 0};

    //表的schema信息
    roachpb::PushDownExpr push_expr = buildPushSchema3();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);
    push_expr.add_required_cols(3);
    push_expr.add_required_cols(4);
    push_expr.add_required_cols(5);
    push_expr.add_required_cols(6);
    push_expr.add_required_cols(7);
    push_expr.add_required_cols(8);

    auto push_str = push_expr.SerializeAsString();
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), 100, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));
    ASSERT_EQ(rs.count(), 6);// 行数
    ASSERT_EQ(rs.col_ids_size(), 8);// 需要返回的列数
    ASSERT_EQ(rs.mutable_col_values(1)->v_int(2), 41234785645);// 第2列的第三行
}


TEST(VecScan, index) {
    /*
     index 索引 生成的kv测试
     */
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();
    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);

    /* 非主键索引
     CREATE TABLE lineitem (l_orderkey INT NOT NULL,
    l_partkey INT NOT NULL,
    l_suppkey INT NOT NULL,
    l_linenumber INT NOT NULL,
    PRIMARY KEY(l_orderkey),
    INDEX l_ok(l_partkey ASC));
    */
    /*
     * 第一列为INT主键,第二列为INT,第三列为INT,第四列为INT,s索引为第二列
     * 12345    12890  124764    124689
     * 52345    89756  23806     24589
     * 5842135  12345  541285645 236789
     *
    */
    ///Table/54/1/12345/
    unsigned char data1_key[] = {190, 137, 247, 48, 57, 136};
    unsigned char data1_value[] = {140, 233, 144, 62, 10, 35, 180, 201, 1, 19, 184, 157, 15, 19, 162, 156, 15};//14
    ///Table/54/1/52345
    unsigned char data2_key[] = {190, 137, 247, 204, 121, 136};
    unsigned char data2_value[] = {214, 179, 13, 232, 10, 35, 184, 250, 10, 19, 252, 243, 2, 19, 154, 128, 3};//16
    ///Table/54/1/5842135
    unsigned char data3_key[] = {190, 137, 248, 89, 36, 215, 136};
    unsigned char data3_value[] = {182, 220, 43, 52, 10, 35, 242, 192, 1, 19, 154, 244, 154, 132, 4, 19, 234, 243, 28};

    ///Table/54/2/12345/5842135
    unsigned char data4_key[] = {190, 138, 247, 48, 57, 248, 89, 36, 215, 136};
    unsigned char data4_value[] = {220, 251, 14, 229, 3};
    ///Table/54/2/12890/12345
    unsigned char data5_key[] = {190, 138, 247, 50, 90, 247, 48, 57, 136};
    unsigned char data5_value[] = {172, 95, 50, 158, 3};
    ///Table/54/2/89756/52345/
    unsigned char data6_key[] = {190, 138, 248, 1, 94, 156, 247, 204, 121, 136};
    unsigned char data6_value[] = {152, 107, 149, 186, 3};
    // Put数据
    // KV数据
    DBPut(db, DBKey{DBSlice{(char *) data1_key, sizeof(data1_key)}, 100, 0},
          DBSlice{(char *) data1_value, sizeof(data1_value)});

    DBPut(db, DBKey{DBSlice{(char *) data2_key, sizeof(data2_key)}, 100, 0},
          DBSlice{(char *) data2_value, sizeof(data2_value)});
    DBPut(db, DBKey{DBSlice{(char *) data3_key, sizeof(data3_key)}, 100, 0},
          DBSlice{(char *) data3_value, sizeof(data3_value)});

    DBPut(db, DBKey{DBSlice{(char *) data4_key, sizeof(data4_key)}, 100, 0},
          DBSlice{(char *) data4_value, sizeof(data4_value)});

    DBPut(db, DBKey{DBSlice{(char *) data5_key, sizeof(data5_key)}, 100, 0},
          DBSlice{(char *) data5_value, sizeof(data5_value)});

    DBPut(db, DBKey{DBSlice{(char *) data6_key, sizeof(data6_key)}, 100, 0},
          DBSlice{(char *) data6_value, sizeof(data6_value)});

    unsigned char start_char[] = {190, 137};
    unsigned char end_char[] = {190, 138};
    DBSlice start = DBSlice{(char *) start_char, 2};
    DBSlice end = DBSlice{(char *) end_char, 2};
    DBTimestamp ts{10000, 0};

    //表的schema信息
    roachpb::PushDownExpr push_expr = buildPushSchema4();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);

    auto push_str = push_expr.SerializeAsString();
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), 100, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));
    ASSERT_EQ(rs.count(), 3);// 行数
    ASSERT_EQ(rs.col_ids_size(), 2);// 需要返回的列数
    ASSERT_EQ(rs.mutable_col_values(1)->v_int(2), 12345);// 第2列的第三行的值
}

TEST(VecScan, UUID_STRING) {
/*
 * 测试uuid的解码以及转成16进制以string形式加入col_values->add_values中. uuid为非主键
 */
/*
 * 第一列为主键int 第二列为UUID类型
 * 1125 63616665-6630-3064-6465-616462656562
 * 1134 63616665-6630-3064-6465-616463457562
 * 1136 0000d32b-896f-4f4f-925b-360ace2f18d8
 */
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();
    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);

    unsigned char data1_key[] = {190, 137, 247,4,101,136};
    unsigned char data1_value[] = {60,134,2,159,10,44,99,97,102,101,102,48,48,100,100,101,97,100,98,101,101,98};

    unsigned char data2_key[] = {190, 137, 247,4,110,136};
    unsigned char data2_value[] = {196,199,170,51,10,44,99,97,102,101,102,48,48,100,100,101,97,100,99,69,117,98};

    unsigned char data3_key[] = {190, 137, 247,4,112,136};
    unsigned char data3_value[] = {28,14,48,200,10,44,0,0,211,43,137,111,79,79,146,91,54,10,206,47,24,216};

    // Put数据
    // KV数据
    DBPut(db, DBKey{DBSlice{(char *) data1_key, sizeof(data1_key)}, 100, 0},
          DBSlice{(char *) data1_value, sizeof(data1_value)});
    DBPut(db, DBKey{DBSlice{(char *) data2_key, sizeof(data2_key)}, 100, 0},
          DBSlice{(char *) data2_value, sizeof(data2_value)});
    DBPut(db, DBKey{DBSlice{(char *) data3_key, sizeof(data3_key)}, 100, 0},
          DBSlice{(char *) data3_value, sizeof(data3_value)});

    unsigned char start_char[] = {190};
    unsigned char end_char[] = {191};
    DBSlice start = DBSlice{(char *) start_char, 1};
    DBSlice end = DBSlice{(char *) end_char, 1};
    DBTimestamp ts{10000, 0};

    //表的schema信息
    roachpb::PushDownExpr push_expr = buildPushSchema6();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);

    auto push_str = push_expr.SerializeAsString();
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), 100, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));
    ASSERT_EQ(rs.count(), 3);// 行数
    ASSERT_EQ(rs.col_ids_size(), 2);// 需要返回的列数
    ASSERT_EQ(rs.mutable_col_values(0)->v_int(1), 1134);// 第1列的第2行的值
}

TEST(VecScan, UUID_primary) {
/*
 * 测试uuid的解码以及转成16进制以string形式加入col_values->add_values中. uuid为主键时
 */
/*
 * 第一列为主键UUID 第二列为int类型
 * 0000d32b-896f-4f4f-925b-360ace2f18d8  1136
 * 63616665-6630-3064-6465-616462656562  1125
 * 63616665-6630-3064-6465-616463457562  1134
 */
    TempDirHandler dir;
    DBOptions db_opts = defaultDBOptions();
    DBEngine *db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);

    unsigned char data1_key[] = {190, 137, 18,99,97,102,101,102,48,48,100,100,101,97,100,98,101,101,98,0,1,136};
    unsigned char data1_value[] = {60,134,2,159,10,35,202,17};

    unsigned char data2_key[] = {190, 137, 18,99,97,102,101,102,48,48,100,100,101,97,100,99,69,117,98,0,1,136};
    unsigned char data2_value[] = {196,199,170,51,10,35,220,17};

    unsigned char data3_key[] = {190, 137, 18,0,255,0,255,211,43,137,111,79,79,146,91,54,10,206,47,24,216,0,1,136};
    unsigned char data3_value[] = {16,113,103,177,10,35,224,17};

    // Put数据
    // KV数据
    DBPut(db, DBKey{DBSlice{(char *) data1_key, sizeof(data1_key)}, 100, 0},
          DBSlice{(char *) data1_value, sizeof(data1_value)});
    DBPut(db, DBKey{DBSlice{(char *) data2_key, sizeof(data2_key)}, 100, 0},
          DBSlice{(char *) data2_value, sizeof(data2_value)});
    DBPut(db, DBKey{DBSlice{(char *) data3_key, sizeof(data3_key)}, 100, 0},
          DBSlice{(char *) data3_value, sizeof(data3_value)});

    unsigned char start_char[] = {190};
    unsigned char end_char[] = {191};
    DBSlice start = DBSlice{(char *) start_char, 1};
    DBSlice end = DBSlice{(char *) end_char, 1};
    DBTimestamp ts{10000, 0};

    //表的schema信息
    roachpb::PushDownExpr push_expr = buildPushSchema7();
    push_expr.add_required_cols(1);
    push_expr.add_required_cols(2);

    auto push_str = push_expr.SerializeAsString();
    DBVecResults results = VecScan(db, start, end, ts, ToDBSlice(push_str), 100, DBTxn{}, false, false, false, false, false);

    znbase::roachpb::VecResults rs;
    ASSERT_TRUE(rs.ParseFromArray(results.data.data, results.data.len));
    ASSERT_EQ(rs.count(), 3);// 行数
    ASSERT_EQ(rs.col_ids_size(), 2);// 需要返回的列数
    ASSERT_EQ(rs.mutable_col_values(0)->values(1), "63616665-6630-3064-6465-616462656562");// 第1列的第2行的值
}