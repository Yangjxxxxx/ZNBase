//
// Created by fxy on 2020/12/10.
//

#ifndef PLSQL_BEPITIME_H
#define PLSQL_BEPITIME_H

#include <time.h>

#define DATE 0
#define TIME 1
#define TIMESTAMP 2
#define TIMESTAMPTZ 3

typedef struct {
    long long secs;
    int microseconds;
} BEPI_INTERVAL;

typedef long BEPI_DATE;

typedef long BEPI_TIME;

typedef struct {
    struct tm tm;
    int time_type;
    int microseconds;
} BEPI_TIMESTAMP;

typedef struct {
    struct tm tm;
    int time_type;
    int microseconds;
} BEPI_TIMESTAMPTZ;

BEPI_TIMESTAMP *BEPI_timestamp_new();

BEPI_TIMESTAMP *BEPI_str2ts(char *str);

BEPI_TIMESTAMPTZ *BEPI_timestamptz_new();

BEPI_TIMESTAMPTZ *BEPI_str2tstz(char *str);

char *BEPI_ts2str(const char *, const BEPI_TIMESTAMP *);

char *BEPI_tstz2str(const char *, const BEPI_TIMESTAMPTZ *);

extern char * BepiCtime2str(Handler_t ,BEPI_TIMESTAMP,char **) ;

#endif //PLSQL_BEPITIME_H
