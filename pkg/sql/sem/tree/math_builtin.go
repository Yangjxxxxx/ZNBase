package tree

import "math"

const (
	//AcosAndAsinBase a base const
	AcosAndAsinBase = 0.5
	//AtandBase atand base
	AtandBase = 1
	//RadiansPerDegree = PI/180
	RadiansPerDegree = 0.0174532925199432957692
)

//Mathematical built-in function logic realization part

//Asind compute asind
func Asind(x float64) float64 {
	if x <= 0.5 {
		asinX := math.Asin(x)
		return (asinX / math.Asin(AcosAndAsinBase)) * 30.0
	}
	acosX := math.Acos(x)
	return 90.0 - (acosX/math.Acos(AcosAndAsinBase))*60.0
}

//Acosd compute acosd
func Acosd(x float64) float64 {
	if x <= 0.5 {
		asinX := math.Asin(x)
		return 90.0 - (asinX/math.Asin(AcosAndAsinBase))*30.0
	}
	acosX := math.Acos(x)
	return (acosX / math.Acos(AcosAndAsinBase)) * 60.0
}

//Atand compute Atand
func Atand(x float64) float64 {
	atandRes := math.Atan(x)
	return (atandRes / math.Atan(AtandBase)) * 45.0
}

//Atan2d compute Atan2d
func Atan2d(x float64, y float64) float64 {
	atan2 := math.Atan2(x, y)
	return (atan2 / math.Atan(AtandBase)) * 45.0
}

//Sind math sin
func Sind(x float64) float64 {
	/*
	 * Stitch together the sine and cosine functions for the ranges [0, 30]
	 * and (30, 90].  These guarantee to return exact answers at their
	 * endpoints, so the overall result is a continuous monotonic function
	 * that gives exact results when x = 0, 30 and 90 degrees.
	 */
	if x <= 30.0 {
		return sind0To30(x)
	}
	return cosd0To60(90.0 - x)
}

//Cosd math cos
func Cosd(x float64) float64 {
	/*
	 * Stitch together the sine and cosine functions for the ranges [0, 60]
	 * and (60, 90].  These guarantee to return exact answers at their
	 * endpoints, so the overall result is a continuous monotonic function
	 * that gives exact results when x = 0, 60 and 90 degrees.
	 */
	if x <= 60.0 {
		return cosd0To60(x)
	}
	return sind0To30(90.0 - x)
}

func sind0To30(x float64) float64 {
	sinx := math.Sin(x * RadiansPerDegree)
	return (sinx / AcosAndAsinBase) / 2.0
}

func cosd0To60(x float64) float64 {
	oneMinusCosx := 1.0 - math.Cos(x*RadiansPerDegree)
	oneMinusCos60 := 1.0 - AcosAndAsinBase
	return 1.0 - (oneMinusCosx/oneMinusCos60)/2.0
}

//CheckInf check if it is infinity
func CheckInf(x float64) bool {
	switch {
	case math.IsNaN(x):
		return false // return ±0 || NaN()
	case math.IsInf(x, 0):
		return false
	}
	return true
}
