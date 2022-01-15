//
// Created by weili on 2021/8/24.
//

#ifndef ROACHLIB_VEC_FILTER_H
#define ROACHLIB_VEC_FILTER_H

#pragma once

#include <vector>
#include <cstring>

//处理filter基本运算的模板函数
//范围校准宏
#define ADJUST_INDICES(start, end, len)     \
    if (end > len)                          \
        end = len;                          \
    else if (end < 0) {                     \
        end += len;                         \
        if (end < 0)                        \
        end = 0;                            \
    }                                       \
    if (start < 0) {                        \
        start += len;                       \
        if (start < 0)                      \
        start = 0;                          \
    }


namespace znbase {

    //匹配函数：endswith与startwith的内部调用函数
    int _string_tailmatch(const std::string&self, const std::string&substr, int start, int end, int direction)
    {
        int selflen = (int)self.size();
        int slen = (int)substr.size();

        const char* str = self.c_str();
        const char* sub = substr.c_str();

        //对输入的范围进行校准
        ADJUST_INDICES(start, end, selflen);

        //字符串头部匹配（即startswith）
        if (direction < 0)
        {
            if (start + slen>selflen)
                return 0;
        }
            //字符串尾部匹配（即endswith）
        else
        {
            if (end - start<slen || start>selflen)
                return 0;
            if (end - slen > start)
                start = end - slen;
        }
        if (end - start >= slen)
            //mcmcmp函数用于比较buf1与buf2的前n个字节
            return !std::memcmp(str + start, sub, slen);
        return 0;
    }

    //等于的filter模板函数
    template <typename T>
    inline bool equalto_Function(T &value,T &filter){
        return value == filter ? true: false;
    }

    //大于的filter模板函数
    template <typename T>
    inline bool greater_Function(T &value,T &filter){
        return value > filter ? true: false;
    }

    //大于或者等于的filter模板函数
    template <typename T>
    inline bool equOrGreater_Function(T &value,T &filter){
        return value >= filter ? true: false;
    }

    //小于的filter模板函数
    template <typename T>
    inline bool less_Function(T &value,T &filter){
        return value < filter ? true: false;
    }

    //小于或者等于的filter模板函数
    template <typename T>
    inline bool equOrLess_Function(T &value,T &filter){
        return value <= filter ? true: false;
    }

    //IN的filter模板函数
    template <typename T>
    inline bool In_Function(T &value,std::vector<T> & filters){
        typename std::vector<T>::iterator ret;
        ret=find(filters.begin(),filters.end(),value);
        return ret != filters.end() ? true : false;
    }

    //判断字符串以什么开头
    bool StringStarts(const std::string& str, const std::string& prefix, int start = 0, int end = INT_MAX)
    {
        //调用＿string＿tailmatch函数，参数-1表示字符串头部匹配
        int result = _string_tailmatch(str, prefix, start, end, -1);
        return static_cast<bool>(result);
    }
    //判断字符串以什么结尾
    bool StringEnds(const std::string& str, const std::string& suffix, int start = 0, int end = INT_MAX)
    {
        //调用＿string＿tailmatch函数，参数+1表示字符串尾部匹配
        int result = _string_tailmatch(str, suffix, start, end, +1);
        return static_cast<bool>(result);
    }
    //判断字符串中包含哪个字符串
    bool StringContains(const std::string& str, const std::string& contains){
        std::string::size_type  idx = str.find(contains);
        if(idx == std::string::npos){
            return false;
        }else{
            return true;
        }
    }
}

#endif //ROACHLIB_VEC_FILTER_H