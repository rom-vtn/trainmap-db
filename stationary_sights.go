package trainmapdb

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/jftuga/geodist"
)

type Point struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// calculates distance between 2 points in km
func (p Point) getDistTo(other Point) float64 {
	_, km := geodist.HaversineDistance(geodist.Coord(p), geodist.Coord(other))
	return km
}

// copying the types from previous implementation
type TrainSight struct {
	ServiceId   string   `json:"service_id"`
	TripId      string   `json:"trip_id"`
	FeedId      string   `json:"feed_id"`
	Feed        *Feed    `json:"feed"`
	StBefore    StopTime `json:"st_before"`
	StAfter     StopTime `json:"st_after"`
	FirstSt     StopTime `json:"first_st"`
	LastSt      StopTime `json:"last_st"`
	Trip        Trip     `json:"trip"`
	RouteName   string   `json:"route_name"`
	passingTime time.Duration
}

type RealTrainSight struct {
	TrainSight TrainSight `json:"sight"`
	Timestamp  time.Time  `json:"timestamp"`
	Date       time.Time  `json:"date"`
}

func (rts *RealTrainSight) updateInnerDates(tz *time.Location) {
	_, offSecs := rts.Timestamp.In(tz).Zone()
	rts.Timestamp = rts.Timestamp.In(tz).Add(-time.Duration(offSecs) * time.Second)
	rts.TrainSight.StBefore.updateDate(rts.Date, tz)
	rts.TrainSight.StAfter.updateDate(rts.Date, tz)
	rts.TrainSight.FirstSt.updateDate(rts.Date, tz)
	rts.TrainSight.LastSt.updateDate(rts.Date, tz)
	newTripStopTimes := make([]StopTime, 0, len(rts.TrainSight.Trip.StopTimes))
	for _, st := range rts.TrainSight.Trip.StopTimes {
		st.updateDate(rts.Date, tz)
		newTripStopTimes = append(newTripStopTimes, st)
	}
	rts.TrainSight.Trip.StopTimes = newTripStopTimes
	rts.Date = updateDateTz(rts.Timestamp, time.Time{}, tz)
}

// returns (day before start date 00:00, start date + dayCount 00:00). Start date is the date of startDateTime if nonzero, today otherwise
func GetDateInterval(dayCount uint, startDateTime time.Time) (Date, Date) {
	if startDateTime.IsZero() {
		startDateTime = time.Now()
	}
	ONE_DAY := 24 * time.Hour
	intervalDuration := time.Duration(dayCount+1) * ONE_DAY // add day to account for rollback
	startDate := startDateTime.Add(-ONE_DAY)                // rollback to start yesterday
	startDate = startDate.Truncate(ONE_DAY)                 // 00:00
	endDate := startDate.Add(intervalDuration)              // get end
	return Date(startDate), Date(endDate)
}

func (stop Stop) GetPoint() Point {
	return Point{Lat: stop.StopLat, Lon: stop.StopLon}
}

// represents a bearing in radians
type Bearing float64

func (pt Point) GetBearingFrom(obsPt Point) Bearing {
	latDiff := pt.Lat - obsPt.Lat
	lonDiff := pt.Lon - obsPt.Lon
	return Bearing(math.Atan2(latDiff, lonDiff))
}

func (bearing Bearing) isDiffLessThan(other Bearing, threshold Bearing) bool {
	absDiff := math.Abs(float64(bearing) - float64(other))
	if absDiff > math.Pi {
		absDiff = 2*math.Pi - absDiff
	}
	return absDiff < float64(threshold)
}

// stA.getTimesWith(stB) should return stA's departure time and stB's arrival
func (st StopTime) getTimesWith(other StopTime) (time.Time, time.Time, error) {
	var startTime, endTime time.Time
	if !st.DepartureTime.IsZero() {
		startTime = st.DepartureTime
	} else {
		if !st.ArrivalTime.IsZero() {
			startTime = st.ArrivalTime
		} else {
			return time.Time{}, time.Time{}, fmt.Errorf("start StopTime has no departure or arrival time")
		}
	}

	if !other.ArrivalTime.IsZero() {
		endTime = other.ArrivalTime
	} else {
		if !other.DepartureTime.IsZero() {
			endTime = other.DepartureTime
		} else {
			return time.Time{}, time.Time{}, fmt.Errorf("end StopTime has no departure or arrival time")
		}
	}

	return startTime, endTime, nil
}

// Computes the estimated time of passing between two StopTime structs.
// FIXME: WARN: DOES NOT CHECK IF THE STOPTIMES ARE ACTUALLY ON THE SAME TRIP/SERVICE
func (st StopTime) getPassingTime(obsPoint Point, other StopTime) (time.Duration, error) {
	// do a simple linear interpolation

	startTime, endTime, err := st.getTimesWith(other)
	if err != nil {
		return time.Duration(0), err
	}
	totalTime := endTime.Sub(startTime)
	totalLat := other.Stop.StopLat - st.Stop.StopLat
	totalLon := other.Stop.StopLon - st.Stop.StopLon

	partialLat := obsPoint.Lat - st.Stop.StopLat
	partialLon := obsPoint.Lon - st.Stop.StopLon
	latProportion := partialLat / totalLat
	lonProportion := partialLon / totalLon
	proportion := lonProportion
	if lonProportion < 0 || lonProportion > 1 {
		proportion = latProportion
	}

	partialTime := time.Duration(int64(float64(totalTime) * proportion))

	return startTime.Add(partialTime).Sub(time.Unix(0, 0)), nil
}

// Checks if the trip in question is a sight at the coords given.
func (f *Fetcher) getPossibleTrainSight(obsPoint Point, trip Trip) (sight TrainSight, hasSight bool, err error) {
	// first, exclude all routes that aren't rail (looking at you buses)
	if !trip.Route.RouteType.isRailType() {
		return TrainSight{}, false, nil
	}

	var stBefore StopTime
	for i, stopTime := range trip.StopTimes {
		hasCloseStop := stopTime.Stop.GetPoint().getDistTo(obsPoint) < f.Config.CloseHeavyRailStationThreshold
		// if not first, check diffs
		if i != 0 {
			//handle source once if we're at the very start
			hasCloseStop = hasCloseStop || trip.StopTimes[0].Stop.GetPoint().getDistTo(obsPoint) < f.Config.CloseHeavyRailStationThreshold
			startPoint := stBefore.Stop.GetPoint()
			endPoint := stopTime.Stop.GetPoint()
			startBearing := startPoint.GetBearingFrom(obsPoint)
			endBearing := endPoint.GetBearingFrom(obsPoint)
			// min threshold (be gracious for now)
			hasCloseStop = hasCloseStop || stopTime.Stop.GetPoint().getDistTo(obsPoint) < f.Config.CloseHeavyRailStationThreshold
			hasBearing := !endBearing.isDiffLessThan(startBearing, f.Config.BearingMaxThreshold)
			if hasBearing || hasCloseStop {
				passingTime, err := stBefore.getPassingTime(obsPoint, stopTime)
				if err != nil {
					return TrainSight{}, false, err
				}
				ts := TrainSight{
					ServiceId:   trip.RefServiceId,
					TripId:      trip.TripId,
					FeedId:      trip.FeedId,
					Feed:        trip.Feed,
					StBefore:    stBefore,
					StAfter:     stopTime,
					FirstSt:     trip.StopTimes[0],
					LastSt:      trip.StopTimes[len(trip.StopTimes)-1],
					Trip:        trip,
					RouteName:   trip.Route.RouteShortName,
					passingTime: passingTime,
				}
				return ts, true, nil
			}
		}

		//shift previous values
		stBefore = stopTime
	}
	return TrainSight{}, false, nil
}

type Date time.Time

func NewDate(day time.Time) Date {
	return Date(day.Truncate(24 * time.Hour))
}

// Fetches train sights at an observation point between starting at startDate's services and endDate (including endDate's GTFS services)
func (f Fetcher) GetRealTrainSights(obsPoint Point, startDate Date, endDate Date) ([]RealTrainSight, error) {
	dateToServices := make(map[time.Time][]FeededService, 0)

	//get all date possibilities
	startDateAsTime := time.Time(startDate)
	endDateAsTime := time.Time(endDate)

	servicesInInterval, err := f.GetServicesBetweenDates(startDateAsTime, endDateAsTime)
	if err != nil {
		return nil, err
	}
	for _, serviceDay := range servicesInInterval {
		date := serviceDay.Date
		dateToServices[date] = append(dateToServices[date], serviceDay.GetFeededService())
	}
	// now we have a map of (date) --> ([]FeededService)
	// need to make a map of (FeededService) --> ([]TrainSight)

	//fetch all trips matching our coords

	possibleTrips, err := f.GetTripsContaining(obsPoint)
	if err != nil {
		return nil, err
	}

	serviceToSights := make(map[FeededService][]TrainSight)

	for _, possibleTrip := range possibleTrips {
		possibleTrainSight, hasSight, err := f.getPossibleTrainSight(obsPoint, possibleTrip)
		if err != nil {
			return nil, err
		}
		if !hasSight {
			continue //skip if not a sight
		}
		feededService := FeededService{FeedId: possibleTrip.FeedId, ServiceId: possibleTrip.RefServiceId}

		//if it is, add to our map
		serviceToSights[feededService] = append(serviceToSights[feededService], possibleTrainSight)
	}

	tz, err := time.LoadLocation(f.Config.TimeZone)
	if err != nil {
		return nil, err
	}

	//cross reference our possible trips to check for trips that cross us
	realTrainSights := []RealTrainSight{}
	for date, feededServices := range dateToServices {
		for _, feededService := range feededServices {
			for _, trainSight := range serviceToSights[feededService] {
				realTrainSight := RealTrainSight{
					TrainSight: trainSight,
					Date:       date,
					Timestamp:  date.Add(trainSight.passingTime),
				}
				realTrainSight.updateInnerDates(tz)
				realTrainSights = append(realTrainSights, realTrainSight)
			}
		}
	}

	sort.Slice(realTrainSights, func(i, j int) bool {
		return realTrainSights[i].Timestamp.Before(realTrainSights[j].Timestamp)
	})

	return realTrainSights, nil
}
