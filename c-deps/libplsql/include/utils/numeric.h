#ifndef TYPES_NUMERIC
#define TYPES_NUMERIC

#define NUMERIC_POS						0x0000
#define NUMERIC_NEG						0x4000
#define NUMERIC_NAN						0xC000
#define NUMERIC_NULL					0xF000
#define NUMERIC_MAX_PRECISION			1000
#define NUMERIC_MAX_DISPLAY_SCALE		NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE		0
#define NUMERIC_MIN_SIG_DIGITS			16

#define TYPES_NUM_OVERFLOW		301
#define TYPES_NUM_BAD_NUMERIC	302
#define TYPES_NUM_DIVIDE_ZERO	303
#define TYPES_NUM_UNDERFLOW		304

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_SHORT		0x8000
#define DECSIZE 30


typedef unsigned char NumericDigit;
typedef struct
{
	int			ndigits;		/* number of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			rscale;			/* result scale */
	int			dscale;			/* display scale */
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	NumericDigit *buf;			/* start of alloc'd space for digits[] */
	NumericDigit *digits;		/* decimal digits */
} numeric;

typedef struct
{
	int			ndigits;		/* number of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			rscale;			/* result scale */
	int			dscale;			/* display scale */
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	NumericDigit digits[DECSIZE];	/* decimal digits */
} decimal;


extern  numeric    *TYPESnumeric_new(void);
extern  decimal    *TYPESdecimal_new(void);
extern  void		TYPESnumeric_free(numeric *);
extern  void		TYPESdecimal_free(decimal *);
extern  numeric    *TYPESnumeric_from_asc(char *, char **);
extern  char	   *TYPESnumeric_to_asc(numeric *, int);
extern  int			TYPESnumeric_add(numeric *, numeric *, numeric *);
extern  int			TYPESnumeric_sub(numeric *, numeric *, numeric *);
extern  int			TYPESnumeric_mul(numeric *, numeric *, numeric *);
extern  int			TYPESnumeric_div(numeric *, numeric *, numeric *);
extern  int			TYPESnumeric_cmp(numeric *, numeric *);
extern  int			TYPESnumeric_from_int(signed int, numeric *);
extern  int			TYPESnumeric_from_long(signed long int, numeric *);
extern  int			TYPESnumeric_copy(numeric *, numeric *);
extern  int			TYPESnumeric_from_double(double, numeric *);
extern  int			TYPESnumeric_to_double(numeric *, double *);
extern  int			TYPESnumeric_to_int(numeric *, int *);
extern  int			TYPESnumeric_to_long(numeric *, long *);
extern  int			TYPESnumeric_to_decimal(numeric *, decimal *);
extern  int			TYPESnumeric_from_decimal(decimal *, numeric *);


#endif							/* TYPES_NUMERIC */
