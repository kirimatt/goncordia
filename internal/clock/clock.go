// Package clock preserves the old internal import path.
package clock

import public "github.com/kirimatt/goncordia/clock"

type Clock = public.Clock
type Ticker = public.Ticker
type Timer = public.Timer
type Real = public.Real
type Manual = public.Manual
type Mock = public.Mock

var NewManual = public.NewManual
var NewMock = public.NewMock
