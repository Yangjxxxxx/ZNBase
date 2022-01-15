//
// Created by fxy on 2020/12/10.
//

#include "plpgsql.h"

BEPI_TIMESTAMP* BEPI_timestamp_new(){
    BEPI_TIMESTAMP * ts = (BEPI_TIMESTAMP*)malloc(sizeof(BEPI_TIMESTAMP));
    ts->tm.tm_year = 0;
    ts->tm.tm_mon = 0;
    ts->tm.tm_mday = 0;
    ts->tm.tm_hour = 0;
    ts->tm.tm_min = 0;
    ts->tm.tm_sec = 0;
    ts->tm.tm_gmtoff = 0;
    ts->tm.tm_isdst = -1;
    ts->tm.tm_wday = 0;
    ts->tm.tm_yday = 0;
    ts->tm.tm_zone = (char*)malloc(sizeof(char)*128);
    memset(ts->tm.tm_zone,'\0',128);
    ts->microseconds = 0;
    return ts;
}

BEPI_TIMESTAMP* BEPI_str2ts(char * str){
    int length = strlen(str);
    BEPI_TIMESTAMP *ts = BEPI_timestamp_new();
    char flag;
    int offset_hour = 0;
    int offset_min = 0;
    if(length == 25){
        sscanf(str,"%d-%d-%d %d:%d:%d%c%d:%d",&ts->tm.tm_year,
               &ts->tm.tm_mon,
               &ts->tm.tm_mday,
               &ts->tm.tm_hour,
               &ts->tm.tm_min,
               &ts->tm.tm_sec,
               &flag,
               &offset_hour,
               &offset_min);
    }else{
        sscanf(str,"%d-%d-%d %d:%d:%d.%d%c%d:%d",&ts->tm.tm_year,
               &ts->tm.tm_mon,
               &ts->tm.tm_mday,
               &ts->tm.tm_hour,
               &ts->tm.tm_min,
               &ts->tm.tm_sec,
               &ts->microseconds,
               &flag,
               &offset_hour,
               &offset_min);

    }
    int offset = 3600*offset_hour + 60 *offset_min;
    ts->tm.tm_gmtoff = offset;
    if (flag == '-'){
        offset = -offset;
    }
    ts->tm.tm_year-=1900;
    ts->tm.tm_mon--;
    return ts;
}
char *BEPI_ts2str(const char *format, const BEPI_TIMESTAMP *ts){
    char * str = (char *)malloc(sizeof(char) * 128);
    strftime(str,128,format,&ts->tm);
    return str;
}
