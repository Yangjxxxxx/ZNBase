/*-------------------------------------------------------------------------
 *
 * string.c
 *		string handling helpers
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/string.c
 *
 *-------------------------------------------------------------------------
 */


#include "plpgsql.h"
#include "common/kwlookup.h"
static const char ScanKeywords_kw_string[] =
	"abort\0"
	"absolute\0"
	"access\0"
	"action\0"
	"add\0"
	"admin\0"
	"after\0"
	"aggregate\0"
	"all\0"
	"also\0"
	"alter\0"
	"always\0"
	"analyse\0"
	"analyze\0"
	"and\0"
	"any\0"
	"array\0"
	"as\0"
	"asc\0"
	"assertion\0"
	"assignment\0"
	"asymmetric\0"
	"at\0"
	"attach\0"
	"attribute\0"
	"authorization\0"
	"backward\0"
	"before\0"
	"begin\0"
	"between\0"
	"bigint\0"
	"binary\0"
	"bit\0"
	"boolean\0"
	"both\0"
	"by\0"
	"cache\0"
	"call\0"
	"called\0"
	"cascade\0"
	"cascaded\0"
	"case\0"
	"cast\0"
	"catalog\0"
	"chain\0"
	"char\0"
	"character\0"
	"characteristics\0"
	"check\0"
	"checkpoint\0"
	"class\0"
	"close\0"
	"cluster\0"
	"coalesce\0"
	"collate\0"
	"collation\0"
	"column\0"
	"columns\0"
	"comment\0"
	"comments\0"
	"commit\0"
	"committed\0"
	"concurrently\0"
	"configuration\0"
	"conflict\0"
	"connection\0"
	"constraint\0"
	"constraints\0"
	"content\0"
	"continue\0"
	"conversion\0"
	"copy\0"
	"cost\0"
	"create\0"
	"cross\0"
	"csv\0"
	"cube\0"
	"current\0"
	"current_catalog\0"
	"current_date\0"
	"current_role\0"
	"current_schema\0"
	"current_time\0"
	"current_timestamp\0"
	"current_user\0"
	"cursor\0"
	"cycle\0"
	"data\0"
	"database\0"
	"day\0"
	"deallocate\0"
	"dec\0"
	"decimal\0"
	"declare\0"
	"default\0"
	"defaults\0"
	"deferrable\0"
	"deferred\0"
	"definer\0"
	"delete\0"
	"delimiter\0"
	"delimiters\0"
	"depends\0"
	"desc\0"
	"detach\0"
	"dictionary\0"
	"disable\0"
	"discard\0"
	"distinct\0"
	"do\0"
	"document\0"
	"domain\0"
	"double\0"
	"drop\0"
	"each\0"
	"else\0"
	"enable\0"
	"encoding\0"
	"encrypted\0"
	"end\0"
	"enum\0"
	"escape\0"
	"event\0"
	"except\0"
	"exclude\0"
	"excluding\0"
	"exclusive\0"
	"execute\0"
	"exists\0"
	"explain\0"
	"extension\0"
	"external\0"
	"extract\0"
	"false\0"
	"family\0"
	"fetch\0"
	"filter\0"
	"first\0"
	"float\0"
	"following\0"
	"for\0"
	"force\0"
	"foreign\0"
	"forward\0"
	"freeze\0"
	"from\0"
	"full\0"
	"function\0"
	"functions\0"
	"generated\0"
	"global\0"
	"grant\0"
	"granted\0"
	"greatest\0"
	"group\0"
	"grouping\0"
	"groups\0"
	"handler\0"
	"having\0"
	"header\0"
	"hold\0"
	"hour\0"
	"identity\0"
	"if\0"
	"ilike\0"
	"immediate\0"
	"immutable\0"
	"implicit\0"
	"import\0"
	"in\0"
	"include\0"
	"including\0"
	"increment\0"
	"index\0"
	"indexes\0"
	"inherit\0"
	"inherits\0"
	"initially\0"
	"inline\0"
	"inner\0"
	"inout\0"
	"input\0"
	"insensitive\0"
	"insert\0"
	"instead\0"
	"int\0"
	"integer\0"
	"intersect\0"
	"interval\0"
	"into\0"
	"invoker\0"
	"is\0"
	"isnull\0"
	"isolation\0"
	"join\0"
	"key\0"
	"label\0"
	"language\0"
	"large\0"
	"last\0"
	"lateral\0"
	"leading\0"
	"leakproof\0"
	"least\0"
	"left\0"
	"level\0"
	"like\0"
	"limit\0"
	"listen\0"
	"load\0"
	"local\0"
	"localtime\0"
	"localtimestamp\0"
	"location\0"
	"lock\0"
	"locked\0"
	"logged\0"
	"mapping\0"
	"match\0"
	"materialized\0"
	"maxvalue\0"
	"method\0"
	"minute\0"
	"minvalue\0"
	"mode\0"
	"month\0"
	"move\0"
	"name\0"
	"names\0"
	"national\0"
	"natural\0"
	"nchar\0"
	"new\0"
	"next\0"
	"no\0"
	"none\0"
	"not\0"
	"nothing\0"
	"notify\0"
	"notnull\0"
	"nowait\0"
	"null\0"
	"nullif\0"
	"nulls\0"
	"numeric\0"
	"object\0"
	"of\0"
	"off\0"
	"offset\0"
	"oids\0"
	"old\0"
	"on\0"
	"only\0"
	"operator\0"
	"option\0"
	"options\0"
	"or\0"
	"order\0"
	"ordinality\0"
	"others\0"
	"out\0"
	"outer\0"
	"over\0"
	"overlaps\0"
	"overlay\0"
	"overriding\0"
	"owned\0"
	"owner\0"
	"parallel\0"
	"parser\0"
	"partial\0"
	"partition\0"
	"passing\0"
	"password\0"
	"placing\0"
	"plans\0"
	"policy\0"
	"position\0"
	"preceding\0"
	"precision\0"
	"prepare\0"
	"prepared\0"
	"preserve\0"
	"primary\0"
	"prior\0"
	"privileges\0"
	"procedural\0"
	"procedure\0"
	"procedures\0"
	"program\0"
	"publication\0"
	"quote\0"
	"range\0"
	"read\0"
	"real\0"
	"reassign\0"
	"recheck\0"
	"recursive\0"
	"ref\0"
	"references\0"
	"referencing\0"
	"refresh\0"
	"reindex\0"
	"relative\0"
	"release\0"
	"rename\0"
	"repeatable\0"
	"replace\0"
	"replica\0"
	"reset\0"
	"restart\0"
	"restrict\0"
	"returning\0"
	"returns\0"
	"revoke\0"
	"right\0"
	"role\0"
	"rollback\0"
	"rollup\0"
	"routine\0"
	"routines\0"
	"row\0"
	"rows\0"
	"rule\0"
	"savepoint\0"
	"schema\0"
	"schemas\0"
	"scroll\0"
	"search\0"
	"second\0"
	"security\0"
	"select\0"
	"sequence\0"
	"sequences\0"
	"serializable\0"
	"server\0"
	"session\0"
	"session_user\0"
	"set\0"
	"setof\0"
	"sets\0"
	"share\0"
	"show\0"
	"similar\0"
	"simple\0"
	"skip\0"
	"smallint\0"
	"snapshot\0"
	"some\0"
	"sql\0"
	"stable\0"
	"standalone\0"
	"start\0"
	"statement\0"
	"statistics\0"
	"stdin\0"
	"stdout\0"
	"storage\0"
	"stored\0"
	"strict\0"
	"strip\0"
	"subscription\0"
	"substring\0"
	"support\0"
	"symmetric\0"
	"sysid\0"
	"system\0"
	"table\0"
	"tables\0"
	"tablesample\0"
	"tablespace\0"
	"temp\0"
	"template\0"
	"temporary\0"
	"text\0"
	"then\0"
	"ties\0"
	"time\0"
	"timestamp\0"
	"to\0"
	"trailing\0"
	"transaction\0"
	"transform\0"
	"treat\0"
	"trigger\0"
	"trim\0"
	"true\0"
	"truncate\0"
	"trusted\0"
	"type\0"
	"types\0"
	"unbounded\0"
	"uncommitted\0"
	"unencrypted\0"
	"union\0"
	"unique\0"
	"unknown\0"
	"unlisten\0"
	"unlogged\0"
	"until\0"
	"update\0"
	"user\0"
	"using\0"
	"vacuum\0"
	"valid\0"
	"validate\0"
	"validator\0"
	"value\0"
	"values\0"
	"varchar\0"
	"variadic\0"
	"varying\0"
	"verbose\0"
	"version\0"
	"view\0"
	"views\0"
	"volatile\0"
	"when\0"
	"where\0"
	"whitespace\0"
	"window\0"
	"with\0"
	"within\0"
	"without\0"
	"work\0"
	"wrapper\0"
	"write\0"
	"xml\0"
	"xmlattributes\0"
	"xmlconcat\0"
	"xmlelement\0"
	"xmlexists\0"
	"xmlforest\0"
	"xmlnamespaces\0"
	"xmlparse\0"
	"xmlpi\0"
	"xmlroot\0"
	"xmlserialize\0"
	"xmltable\0"
	"year\0"
	"yes\0"
	"zone";

static const uint16 ScanKeywords_kw_offsets[] = {
	0,
	6,
	15,
	22,
	29,
	33,
	39,
	45,
	55,
	59,
	64,
	70,
	77,
	85,
	93,
	97,
	101,
	107,
	110,
	114,
	124,
	135,
	146,
	149,
	156,
	166,
	180,
	189,
	196,
	202,
	210,
	217,
	224,
	228,
	236,
	241,
	244,
	250,
	255,
	262,
	270,
	279,
	284,
	289,
	297,
	303,
	308,
	318,
	334,
	340,
	351,
	357,
	363,
	371,
	380,
	388,
	398,
	405,
	413,
	421,
	430,
	437,
	447,
	460,
	474,
	483,
	494,
	505,
	517,
	525,
	534,
	545,
	550,
	555,
	562,
	568,
	572,
	577,
	585,
	601,
	614,
	627,
	642,
	655,
	673,
	686,
	693,
	699,
	704,
	713,
	717,
	728,
	732,
	740,
	748,
	756,
	765,
	776,
	785,
	793,
	800,
	810,
	821,
	829,
	834,
	841,
	852,
	860,
	868,
	877,
	880,
	889,
	896,
	903,
	908,
	913,
	918,
	925,
	934,
	944,
	948,
	953,
	960,
	966,
	973,
	981,
	991,
	1001,
	1009,
	1016,
	1024,
	1034,
	1043,
	1051,
	1057,
	1064,
	1070,
	1077,
	1083,
	1089,
	1099,
	1103,
	1109,
	1117,
	1125,
	1132,
	1137,
	1142,
	1151,
	1161,
	1171,
	1178,
	1184,
	1192,
	1201,
	1207,
	1216,
	1223,
	1231,
	1238,
	1245,
	1250,
	1255,
	1264,
	1267,
	1273,
	1283,
	1293,
	1302,
	1309,
	1312,
	1320,
	1330,
	1340,
	1346,
	1354,
	1362,
	1371,
	1381,
	1388,
	1394,
	1400,
	1406,
	1418,
	1425,
	1433,
	1437,
	1445,
	1455,
	1464,
	1469,
	1477,
	1480,
	1487,
	1497,
	1502,
	1506,
	1512,
	1521,
	1527,
	1532,
	1540,
	1548,
	1558,
	1564,
	1569,
	1575,
	1580,
	1586,
	1593,
	1598,
	1604,
	1614,
	1629,
	1638,
	1643,
	1650,
	1657,
	1665,
	1671,
	1684,
	1693,
	1700,
	1707,
	1716,
	1721,
	1727,
	1732,
	1737,
	1743,
	1752,
	1760,
	1766,
	1770,
	1775,
	1778,
	1783,
	1787,
	1795,
	1802,
	1810,
	1817,
	1822,
	1829,
	1835,
	1843,
	1850,
	1853,
	1857,
	1864,
	1869,
	1873,
	1876,
	1881,
	1890,
	1897,
	1905,
	1908,
	1914,
	1925,
	1932,
	1936,
	1942,
	1947,
	1956,
	1964,
	1975,
	1981,
	1987,
	1996,
	2003,
	2011,
	2021,
	2029,
	2038,
	2046,
	2052,
	2059,
	2068,
	2078,
	2088,
	2096,
	2105,
	2114,
	2122,
	2128,
	2139,
	2150,
	2160,
	2171,
	2179,
	2191,
	2197,
	2203,
	2208,
	2213,
	2222,
	2230,
	2240,
	2244,
	2255,
	2267,
	2275,
	2283,
	2292,
	2300,
	2307,
	2318,
	2326,
	2334,
	2340,
	2348,
	2357,
	2367,
	2375,
	2382,
	2388,
	2393,
	2402,
	2409,
	2417,
	2426,
	2430,
	2435,
	2440,
	2450,
	2457,
	2465,
	2472,
	2479,
	2486,
	2495,
	2502,
	2511,
	2521,
	2534,
	2541,
	2549,
	2562,
	2566,
	2572,
	2577,
	2583,
	2588,
	2596,
	2603,
	2608,
	2617,
	2626,
	2631,
	2635,
	2642,
	2653,
	2659,
	2669,
	2680,
	2686,
	2693,
	2701,
	2708,
	2715,
	2721,
	2734,
	2744,
	2752,
	2762,
	2768,
	2775,
	2781,
	2788,
	2800,
	2811,
	2816,
	2825,
	2835,
	2840,
	2845,
	2850,
	2855,
	2865,
	2868,
	2877,
	2889,
	2899,
	2905,
	2913,
	2918,
	2923,
	2932,
	2940,
	2945,
	2951,
	2961,
	2973,
	2985,
	2991,
	2998,
	3006,
	3015,
	3024,
	3030,
	3037,
	3042,
	3048,
	3055,
	3061,
	3070,
	3080,
	3086,
	3093,
	3101,
	3110,
	3118,
	3126,
	3134,
	3139,
	3145,
	3154,
	3159,
	3165,
	3176,
	3183,
	3188,
	3195,
	3203,
	3208,
	3216,
	3222,
	3226,
	3240,
	3250,
	3261,
	3271,
	3281,
	3295,
	3304,
	3310,
	3318,
	3331,
	3340,
	3345,
	3349,
};

#define SCANKEYWORDS_NUM_KEYWORDS 442

static int
ScanKeywords_hash_func(const void *key, size_t keylen)
{
	static const int16 h[885] = {
		 32767,   -102,  32767,     39,  32767,  32767,      0,  32767,
		     0,     45,   -314,     45,    279,      0,    463,      0,
		 32767,   -177,    374,  32767,  32767,  32767,    -50,  32767,
		 32767,    103,      0,   1031,  32767,     95,    151,  32767,
		 32767,    209,    345,      0,  32767,      0,     67,  32767,
		 32767,   -168,  32767,  32767,  32767,  32767,  32767,  32767,
		 32767,  32767,  32767,     49,    185,  32767,    102,  32767,
		 32767,    360,    182,    121,    207,  32767,    111,  32767,
		   107,    -63,   -269,  32767,    206,    187,  32767,  32767,
		   256,    109,    224,  32767,  32767,     21,  32767,  32767,
		 32767,  32767,    213,      0,     54,  32767,    341,    227,
		   144,   -397,   -278,   -701,  32767,   -191,    -15,    221,
		 32767,  32767,    241,  32767,    121,     19,  32767,  32767,
		   290,    232,    349,   -265,   -295,  32767,   -203,     97,
		   179,    117,     50,    331,  32767,     48,     57,    310,
		   -88,  32767,  32767,  32767,      0,     36,  32767,    -98,
		    71,  32767,    273,      0,     55,    170,    411,  32767,
		 32767,  32767,    212,    -48,      0,  32767,    323,  32767,
		    79,  32767,  32767,   -628,    102,  32767,    131,  32767,
		  -150,    373,    263,    429,      0,    166,  32767,   -376,
		 32767,   -193,    358,    266,  32767,    257,   -151,    674,
		 32767,    175,    109,  32767,    351,  32767,    229,     11,
		    50,  32767,      0,    172,  32767,    414,    226,    578,
		 32767,  32767,      0,  32767,  32767,     96,     -7,     10,
		   256,  32767,  32767,    100,    191,    104,   -334,   -232,
		 32767,   -217,     88,  32767,     87,    179,    739,    203,
		     8,   -206,    282,    295,  32767,      0,    229,    -49,
		   102,    427,  32767,  32767,    264,  32767,  32767,    209,
		 32767,   -329,  32767,    247,      0,  32767,  32767,  32767,
		 32767,     74,    431,      0,  32767,  32767,     59,  32767,
		    61,  32767,    389,     91,      0,  32767,  32767,      0,
		     0,    322,    396,      7,    557,    120,    348,      4,
		 32767,      0,    377,    117,    311,  32767,     -7,    367,
		  -129,     91,  32767,  32767,      0,     74,      0,    162,
		 32767,      0,  32767,   -367,      0,  32767,    235,      2,
		    42,  32767,   -221,     15,  32767,    308,    224,     16,
		   604,  32767,      0,      4,    301,  32767,   -388,  32767,
		 32767,    235,  32767,      0,     -6,     57,  32767,    503,
		 32767,    135,  32767,  32767,      0,    306,    190,     55,
		   153,    133,    616,   -672,   -197,    172,  32767,  32767,
		   245,    315,     82,     25,    277,  32767,  32767,  32767,
		  -103,    361,      0,  32767,  32767,      0,  32767,  32767,
		   122,  32767,  32767,  32767,   -293,     64,    421,     44,
		   -46,  32767,    401,   -137,    320,  32767,    280,    329,
		   308,   -489,   1013,      0,    240,     22,     73,    306,
		   271,  32767,    334,  32767,    429,      0,    635,  32767,
		     0,  32767,  32767,  32767,    128,      0,      0,    587,
		 32767,  32767,   -222,    152,    323,  32767,  32767,  32767,
		  -103,      0,    408,  32767,  32767,      0,     20,    351,
		    51,  32767,      0,    169,  32767,    309,     88,      0,
		    73,   -734,   -377,  32767,  32767,    136,  32767,  32767,
		 32767,  32767,    119,    -30,    236,  32767,  32767,  32767,
		    18,      0,    394,  32767,  32767,  32767,    154,     79,
		     0,    327,  32767,    296,    -30,  32767,    390,   -577,
		 32767,  32767,      0,    146,      0,    349,  32767,      0,
		   582,    438,    208,  32767,      0,  32767,      0,  32767,
		   -44,    -16,  32767,  32767,  32767,  32767,  32767,     -9,
		 32767,  32767,  32767,  32767,   -124,      0,    209,  32767,
		   436,  32767,    275,     80,    291,  32767,    238,    227,
		     8,    143,  32767,      0,    939,      0,    287,  32767,
		   119,  32767,   -244,  32767,    261,   -117,   -153,    105,
		   108,  32767,   -232,    397,    384,    297,     52,     77,
		     0,  32767,    207,  32767,  32767,      0,    173,  32767,
		   369,  32767,    296,  32767,    183,     27,      0,  32767,
		   141,    393,  32767,  32767,      0,     98,  32767,  32767,
		    63,  32767,  32767,    644,      0,    401,  32767,      0,
		    76,    396,    361,  32767,    118,  32767,   -175,      0,
		     0,      0,    198,    700,    142,    140,  32767,  32767,
		 32767,     36,  32767,    -21,    626,  32767,    367,     18,
		  -150,     69,    152,  32767,    -99,  32767,    335,  32767,
		   364,    -98,  32767,  32767,  32767,  32767,    101,  32767,
		   313,  32767,    320,    211,    249,     10,      0,  32767,
		  -265,  32767,      0,    433,  32767,    330,    343,    423,
		 32767,  32767,     87,     75,      0,  32767,      0,    362,
		   293,   -321,  32767,      0,  32767,  32767,  32767,    435,
		    82,    164,  32767,    291,  32767,    404,      0,  32767,
		     0,  32767,  32767,  32767,  32767,  32767,      0,   -188,
		 32767,    324,   -161,  32767,    106,  32767,     62,    567,
		     0,      0,      0,      0,  32767,      0,    391,  32767,
		     0,    299,  32767,    222,  32767,   -134,     66,  32767,
		    21,  32767,    147,   -269,    346,    347,      0,      0,
		  -184,  32767,    578,    107,    239,  32767,    620,    402,
		   432,  32767,    409,     93,  -1004,      0,  32767,      0,
		 32767,  32767,    -74,      0,  32767,      0,    259,    377,
		 32767,      0,    260,   -128,   -163,      0,     59,    444,
		 32767,   -290,  32767,     92,      0,  32767,    276,    140,
		  -297,  32767,  32767,  32767,  32767,  32767,   -230,    115,
		    -3,     37,    241,    -96,      0,    379,   -415,  32767,
		  -295,  32767,     50,      0,     22,      0,    159,     29,
		   158,  32767,  32767,      0,  32767,    284,    332,    321,
		 32767,  32767,  32767,    352,      0,  32767,    593,      0,
		   414,  32767,      0,      0,      0,  32767,      0,    638,
		 32767,    289,  32767,  32767,  32767,      0,    465,   -289,
		     0,  32767,  32767,    131,    255,  32767,  32767,    -96,
		 32767,  32767,      0,  32767,      0,  32767,  32767,      0,
		     0,    165,    350,  32767,   -372,    252,      0,    -18,
		 32767,  32767,      0,   -173,  32767,  32767,  32767,  32767,
		  -188,      0,     54,    748,    281,  32767,  32767,   -358,
		   424,   -196,  32767,    122,      0,    -61,    223,      0,
		     0,  32767,   -358,      0,  32767,  32767,     26,    316,
		 32767,      0,    213,   -120,  32767,  32767,      0,    156,
		 32767,    316,    430,  32767,  32767,     40,      0,    388,
		   -15,  32767,  32767,      0,   -105,      0,      0,    138,
		   -59,  32767,    245,    570,  32767,      0,  32767,    300,
		 32767,      0,    161,  32767,  32767,      0,  32767,  32767,
		    -4,    258,      0,  32767,    -83,  32767,    349,   -244,
		 32767,    106,    208,      0,  32767,   -195,    200,  32767,
		 32767,  32767,  32767,      0,    374,   -251,  32767,  32767,
		  -201,     85,   -336,    212,  32767,  32767,    285,    344,
		 32767,  32767,  32767,  32767,  32767,  32767,  32767,      0,
		 32767,   -135,  32767,    387,    167,
	};

	const unsigned char *k = (const unsigned char *) key;
	uint32		a = 0;
	uint32		b = 9;

	while (keylen--)
	{
		unsigned char c = *k++ | 0x20;

		a = a * 31 + c;
		b = b * 127 + c;
	}
	return h[a % 885] + h[b % 885];
}

const ScanKeywordList ScanKeywords = {
	ScanKeywords_kw_string,
	ScanKeywords_kw_offsets,
	ScanKeywords_hash_func,
	SCANKEYWORDS_NUM_KEYWORDS,
	17
};

const uint8 ScanKeywordCategories[] = {

};

/*
 * Returns whether the string `str' has the postfix `end'.
 */
bool
pg_str_endswith(const char *str, const char *end)
{
	size_t		slen = strlen(str);
	size_t		elen = strlen(end);

	/* can't be a postfix if longer */
	if (elen > slen)
		return false;

	/* compare the end of the strings */
	str += slen - elen;
	return strcmp(str, end) == 0;
}


/*
 * strtoint --- just like strtol, but returns int not long
 */
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
	long		val;

	val = strtol(str, endptr, base);
	if (val != (int) val)
		errno = ERANGE;
	return (int) val;
}


/*
 * pg_clean_ascii -- Replace any non-ASCII chars with a '?' char
 *
 * Modifies the string passed in which must be '\0'-terminated.
 *
 * This function exists specifically to deal with filtering out
 * non-ASCII characters in a few places where the client can provide an almost
 * arbitrary string (and it isn't checked to ensure it's a valid username or
 * database name or similar) and we don't want to have control characters or other
 * things ending up in the log file where server admins might end up with a
 * messed up terminal when looking at them.
 *
 * In general, this function should NOT be used- instead, consider how to handle
 * the string without needing to filter out the non-ASCII characters.
 *
 * Ultimately, we'd like to improve the situation to not require stripping out
 * all non-ASCII but perform more intelligent filtering which would allow UTF or
 * similar, but it's unclear exactly what we should allow, so stick to ASCII only
 * for now.
 */
void
pg_clean_ascii(char *str)
{
	/* Only allow clean ASCII chars in the string */
	char	   *p;

	for (p = str; *p != '\0'; p++)
	{
		if (*p < 32 || *p > 126)
			*p = '?';
	}
}

/*
 * psprintf
 *
 * Format text data under the control of fmt (an sprintf-style format string)
 * and return it in an allocated-on-demand buffer.  The buffer is allocated
 * with palloc in the backend, or malloc in frontend builds.  Caller is
 * responsible to free the buffer when no longer needed, if appropriate.
 *
 * Errors are not returned to the caller, but are reported via elog(ERROR)
 * in the backend, or printf-to-stderr-and-exit() in frontend builds.
 * One should therefore think twice about using this in libpq.
 */
char *
psprintf(const char *fmt,...)
{
	int			save_errno = errno;
	size_t		len = 128;		/* initial assumption about buffer size */

	for (;;)
	{
		char	   *result;
		va_list		args;
		size_t		newlen;

		/*
		 * Allocate result buffer.  Note that in frontend this maps to malloc
		 * with exit-on-error.
		 */
		result = (char *) palloc(len);

		/* Try to format the data. */
		errno = save_errno;
		va_start(args, fmt);
		newlen = pvsnprintf(result, len, fmt, args);
		va_end(args);

		if (newlen < len)
			return result;		/* success */

		/* Release buffer and loop around to try again with larger len. */
		pfree(result);
		len = newlen;
	}
}

/*
 * pvsnprintf
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and insert it into buf (which has length len).
 *
 * If successful, return the number of bytes emitted, not counting the
 * trailing zero byte.  This will always be strictly less than len.
 *
 * If there's not enough space in buf, return an estimate of the buffer size
 * needed to succeed (this *must* be more than the given len, else callers
 * might loop infinitely).
 *
 * Other error cases do not return, but exit via elog(ERROR) or exit().
 * Hence, this shouldn't be used inside libpq.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 *
 * Note that the semantics of the return value are not exactly C99's.
 * First, we don't promise that the estimated buffer size is exactly right;
 * callers must be prepared to loop multiple times to get the right size.
 * (Given a C99-compliant vsnprintf, that won't happen, but it is rumored
 * that some implementations don't always return the same value ...)
 * Second, we return the recommended buffer size, not one less than that;
 * this lets overflow concerns be handled here rather than in the callers.
 */
size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			nprinted;

	nprinted = vsnprintf(buf, len, fmt, args);

	/* We assume failure means the fmt is bogus, hence hard failure is OK */
	if (unlikely(nprinted < 0))
	{
#ifndef FRONTEND
		elog(ERROR, "vsnprintf failed: %m with format string \"%s\"", fmt);
#else
		fprintf(stderr, "vsnprintf failed: %s with format string \"%s\"\n",
				strerror(errno), fmt);
		exit(EXIT_FAILURE);
#endif
	}

	if ((size_t) nprinted < len)
	{
		/* Success.  Note nprinted does not include trailing null. */
		return (size_t) nprinted;
	}

	/*
	 * We assume a C99-compliant vsnprintf, so believe its estimate of the
	 * required space, and add one for the trailing null.  (If it's wrong, the
	 * logic will still work, but we may loop multiple times.)
	 *
	 * Choke if the required space would exceed MaxAllocSize.  Note we use
	 * this palloc-oriented overflow limit even when in frontend.
	 */
	if (unlikely((size_t) nprinted > MaxAllocSize - 1))
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of memory")));
#else
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
#endif
	}

	return nprinted + 1;
}

/*
 * quote_identifier			- Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
const char *
quote_identifier(const char *ident)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter or underscore
	 * and contains only lowercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int			nquotes = 0;
	bool		safe;
	const char *ptr;
	char	   *result;
	char	   *optr;

	/*
	 * would like to use <ctype.h> macros here, but they might yield unwanted
	 * locale-specific results...
	 */
	safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');

	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			(ch == '_'))
		{
			/* okay */
		}
		else
		{
			safe = false;
			if (ch == '"')
				nquotes++;
		}
	}

	if (safe)
	{
		/*
		 * Check for keyword.  We quote keywords except for unreserved ones.
		 * (In some cases we could avoid quoting a col_name or type_func_name
		 * keyword, but it seems much harder than it's worth to tell that.)
		 *
		 * Note: ScanKeywordLookup() does case-insensitive comparison, but
		 * that's fine, since we already know we have all-lower-case.
		 */
        // TODO: znbase 的关键字？？？
		int			kwnum = ScanKeywordLookup(ident, &ScanKeywords);

		if (kwnum >= 0 && ScanKeywordCategories[kwnum] != 0)
			safe = false;
	}

	if (safe)
		return ident;			/* no change needed */

	result = (char *) palloc(strlen(ident) + nquotes + 2 + 1);

	optr = result;
	*optr++ = '"';
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if (ch == '"')
			*optr++ = '"';
		*optr++ = ch;
	}
	*optr++ = '"';
	*optr = '\0';

	return result;
}


