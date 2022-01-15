//
// Created by weili on 2021/8/31.
//

#include "vec_scan_data.h"

using namespace znbase;

//前序遍历构建过滤条件
void preBuildFilter(znbase::roachpb::FilterUnion *filter_input,std::stack<znbase::roachpb::FilterType> &filter_Type_vec,std::stack<std::string> &filter_ID_vec,std::stack<std::string> &filter_Value_vec){
    if(filter_Type_vec.empty()){
        return;
    }
    filter_input->set_type(filter_Type_vec.top());
    if(filter_Type_vec.top() < roachpb::FilterType::Not){
        filter_Type_vec.pop();
        znbase::roachpb::BaseFilter *base_Filter = new roachpb::BaseFilter();
        base_Filter->set_attribute(filter_ID_vec.top());//列ID
        filter_ID_vec.pop();
        if(filter_input->type() == znbase::roachpb::In){
            base_Filter->add_values("法师");
            base_Filter->add_values("射手");
        }else if(filter_input->type() == znbase::roachpb::IsNotNull || filter_input->type() == znbase::roachpb::IsNull){


        }else{
            base_Filter->set_value(filter_Value_vec.top());
            filter_Value_vec.pop();
        }
        filter_input->set_allocated_base(base_Filter);
    } else if(filter_Type_vec.top() > roachpb::FilterType::Not){
        filter_Type_vec.pop();
        znbase::roachpb::LogicalFilter *logical_filter = new roachpb::LogicalFilter();
        znbase::roachpb::FilterUnion *filter_left = new roachpb::FilterUnion();
        znbase::roachpb::FilterUnion *filter_right = new roachpb::FilterUnion();
        preBuildFilter(filter_left,filter_Type_vec,filter_ID_vec,filter_Value_vec);
        preBuildFilter(filter_right,filter_Type_vec,filter_ID_vec,filter_Value_vec);
        logical_filter->set_allocated_left(filter_left);
        logical_filter->set_allocated_right(filter_right);
        filter_input->set_allocated_logical(logical_filter);
    }else if(filter_Type_vec.top() == roachpb::FilterType::Not){
        filter_Type_vec.pop();
        znbase::roachpb::LogicalFilter *logical_filter = new roachpb::LogicalFilter();
        znbase::roachpb::FilterUnion *filter_left = new roachpb::FilterUnion();
        preBuildFilter(filter_left,filter_Type_vec,filter_ID_vec,filter_Value_vec);
        logical_filter->set_allocated_left(filter_left);
        filter_input->set_allocated_logical(logical_filter);
    }
}


void PreOrderTest(znbase::roachpb::FilterUnion *filter_union)
{
    if (filter_union->has_base()) {
        // base节点
        // 组织filter条件，对kv进行数据判断
        std::string fil_value = "0";
        znbase::roachpb::FilterType fil_Type =  filter_union->type();
        znbase::roachpb::BaseFilter *base_filter = filter_union->mutable_base();
        std::string str_ID = base_filter->attribute();//比较的id
        int fil_ID = atoi(str_ID.c_str());//比较的id转成int
        if(base_filter->has_value()){
            fil_value = base_filter->value();//过滤条件比较的值
        }
        std::cout << "fil_Type == " << fil_Type << "  fil_ID ==  " << fil_ID << "  fil_value == " << fil_value << std::endl;
    } else if (filter_union->has_logical()) {
        std::cout << "filter_union type == "  <<filter_union->type() << std::endl;
        // 左子节点
        PreOrderTest(filter_union->mutable_logical()->mutable_left());
        // 右子节点
        PreOrderTest(filter_union->mutable_logical()->mutable_right());
    }
}

roachpb::PushDownExpr buildCustomerSchema() {
    roachpb::PushDownExpr push_expr;
    int col_count = 9;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes[i]);
    }
    push_expr.add_primary_cols(9);
    return push_expr;
}

roachpb::PushDownExpr buildLineitemSchema() {//带主键的Lineitem表的schema信息
    roachpb::PushDownExpr push_expr;
    int types[] = {ColumnType_INT, ColumnType_INT, ColumnType_INT, ColumnType_INT,ColumnType_INT,
                   ColumnType_DECIMAL, ColumnType_DECIMAL, ColumnType_DECIMAL,
                   ColumnType_STRING, ColumnType_STRING,
                   ColumnType_INT, ColumnType_INT, ColumnType_INT,
                   ColumnType_STRING, ColumnType_STRING, ColumnType_STRING};
    int col_count = sizeof(types) / sizeof(int);
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types(static_cast<roachpb::ColumnMeta_Type>(types[i]));
    }
    push_expr.add_primary_cols(1);
    push_expr.add_primary_cols(4);
    push_expr.add_primary_cols(11);
    push_expr.add_primary_cols(2);

    return push_expr;
}

roachpb::PushDownExpr buildPushSchema2() {
    roachpb::PushDownExpr push_expr;
    int col_count = 4;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes2[i]);
    }
    push_expr.add_primary_cols(2);
    push_expr.add_primary_cols(3);
    return push_expr;
}

roachpb::PushDownExpr buildPushSchema3() {
    roachpb::PushDownExpr push_expr;
    int col_count = 8;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes3[i]);
    }
    push_expr.add_primary_cols(7);
    push_expr.add_primary_cols(8);
    push_expr.add_primary_cols(1);
    push_expr.add_primary_cols(4);
    push_expr.add_primary_cols(6);
    push_expr.add_primary_cols(2);
    push_expr.add_primary_cols(5);
    return push_expr;
}

roachpb::PushDownExpr buildPushSchema4() {
    roachpb::PushDownExpr push_expr;
    int col_count = 4;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes4[i]);
    }
    push_expr.add_primary_cols(1);
    return push_expr;
}

roachpb::PushDownExpr buildPushSchema6() {
    roachpb::PushDownExpr push_expr;
    int col_count = 2;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes6[i]);
    }
    push_expr.add_primary_cols(1);
    return push_expr;
}

roachpb::PushDownExpr buildPushSchema7() {
    roachpb::PushDownExpr push_expr;
    int col_count = 2;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes7[i]);
    }
    push_expr.add_primary_cols(1);
    return push_expr;
}

roachpb::PushDownExpr buildCustomerDecSchema() {
    roachpb::PushDownExpr push_expr;
    int col_count = 8;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerDecTypes[i]);
    }
    push_expr.add_primary_cols(1);
    znbase::roachpb::FilterUnion *filter_Union = push_expr.add_filters();
    filter_Union->set_type(roachpb::In);
    znbase::roachpb::BaseFilter *base_Filter = new roachpb::BaseFilter();
    base_Filter->set_attribute("6");//列ID
//    base_Filter->set_value("122.25");
    base_Filter->add_values("3857.34");
    base_Filter->add_values("-272.60");
    filter_Union->set_allocated_base(base_Filter);
    return push_expr;
}


roachpb::PushDownExpr buildHeroSchema() {
    roachpb::PushDownExpr push_expr;
    int col_count = 7;
    for (int i = 0; i < col_count; i++) {
        push_expr.add_col_ids(i + 1);
        push_expr.add_col_types((roachpb::ColumnMeta_Type) CustomerTypes9[i]);
    }
    push_expr.add_primary_cols(1);

    std::stack<znbase::roachpb::FilterType> filter_type_vec;//根左右
    filter_type_vec.push(roachpb::FilterType::In);
//    filter_type_vec.push(roachpb::FilterType::Equal);
//    filter_type_vec.push(roachpb::FilterType::Not);
//    filter_type_vec.push(roachpb::FilterType::Or);
    std::stack<std::string> filter_ID_vec;
    filter_ID_vec.push("3");
//    filter_ID_vec.push("2");

    std::stack<std::string> filter_Value_vec;
//    filter_Value_vec.push("tom");
    znbase::roachpb::FilterUnion *filter_Union1 = push_expr.add_filters();
    preBuildFilter(filter_Union1,filter_type_vec,filter_ID_vec,filter_Value_vec);
    return push_expr;
}