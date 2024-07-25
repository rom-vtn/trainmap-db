package trainmapdb

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/dominikbraun/graph/draw"

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

func (rts *RealTrainSight) updateInnerDates() {
	rts.TrainSight.StBefore.updateDate(rts.Date)
	rts.TrainSight.StAfter.updateDate(rts.Date)
	rts.TrainSight.FirstSt.updateDate(rts.Date)
	rts.TrainSight.LastSt.updateDate(rts.Date)
	for i := range rts.TrainSight.Trip.StopTimes {
		rts.TrainSight.Trip.StopTimes[i].updateDate(rts.Date)
	}
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

// returns (dist(point, line(stationA, stationB)), dist(point, line(stationA, stationB)) / dist(stationA, stationB)).
// Formula from https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line#Line_defined_by_two_points
func (p Point) getFractionOfDistTo(pointA, pointB Point) (float64, float64) {
	x0 := p.Lat
	y0 := p.Lon
	x1 := pointA.Lat
	y1 := pointA.Lon
	x2 := pointB.Lat
	y2 := pointB.Lon

	fractionTopSide := math.Abs((x2-x1)*(y0-y1) - (x0-x1)*(y2-y1))
	stationDistSquare := (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1) //dist squared, divide once by dist for formula, then second time for proportion
	stationDist := math.Sqrt(stationDistSquare)
	//first value is "absolute" dist in degrees, second is percentage of dist between stations
	return fractionTopSide / stationDist, fractionTopSide / stationDistSquare
}

// Checks if the trip in question is a sight at the coords given.
func (f *Fetcher) getPossibleTrainSight(nm *NetworkMap, obsPoint Point, trip Trip) (sight TrainSight, hasSight bool, err error) {
	// first, exclude all routes that aren't rail (looking at you buses)
	if !trip.Route.RouteType.isRailType() {
		return TrainSight{}, false, nil
	}

	var stBefore StopTime
	segments := trip.getSegments()
	for i, stopTime := range trip.StopTimes {
		hasCloseStop := stopTime.Stop.GetPoint().getDistTo(obsPoint) < f.Config.CloseHeavyRailStationThreshold
		// if not first, check diffs
		if i != 0 {
			//compute segments
			currentSegment := segments[i-1]
			trajectory, err := nm.decomposeTripSegment(currentSegment)
			if err != nil {
				return TrainSight{}, false, err
			}
			//normalize travel time
			var trajectoryTotalTime time.Duration
			for _, seg := range trajectory {
				trajectoryTotalTime += seg.travelTime
			}
			//handle source once if we're at the very start
			hasCloseStop = hasCloseStop || trajectory[0].source.GetPoint().getDistTo(obsPoint) < f.Config.CloseHeavyRailStationThreshold
			for i, seg := range trajectory {
				if trajectoryTotalTime == 0 {
					trajectory[i].travelTime = 0
				} else {
					trajectory[i].travelTime = (seg.travelTime * currentSegment.travelTime) / trajectoryTotalTime
				}
				startPoint := seg.source.GetPoint()
				endPoint := seg.target.GetPoint()
				startBearing := startPoint.GetBearingFrom(obsPoint)
				endBearing := endPoint.GetBearingFrom(obsPoint)
				// min threshold (be gracious for now)
				hasCloseStop = hasCloseStop || seg.target.GetPoint().getDistTo(obsPoint) < f.Config.CloseHeavyRailStationThreshold
				hasBearing := !endBearing.isDiffLessThan(startBearing, f.Config.BearingMaxThreshold)
				if hasBearing || hasCloseStop {
					fmt.Printf("OK: %s -> %s on segment %s -> %s (%f -> %f) = %f\n", stBefore.Stop.StopName, stopTime.Stop.StopName, seg.source.StopName, seg.target.StopName, math.Mod(float64(startBearing*180/math.Pi), 360), math.Mod(float64(endBearing*180/math.Pi), 360), math.Mod(float64((endBearing-startBearing)*180/math.Pi), 360))
					passingTime, err := stBefore.getPassingTime(obsPoint, stopTime)
					if err != nil {
						return TrainSight{}, false, err
					}
					return TrainSight{
						ServiceId:   trip.ServiceId,
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
					}, true, nil
				}
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
	//DEBUG
	//RESULTS: LOOOKUP INCLUDES OUSIDE BOUNDS
	//hasP34 := false
	//for _, t := range possibleTrips {
	//	routeName := t.Route.RouteShortName
	//	if routeName == "P34" {
	//		hasP34 = true
	//	}
	//}
	//if !hasP34 {
	//	return nil, fmt.Errorf("NO P34 FOUND")
	//}

	nm, err := NewNetworkMapFromTrips(possibleTrips)
	if err != nil {
		return nil, err
	}

	file, _ := os.Create("my-graph.gv")
	draw.DOT(nm.Graph, file)

	serviceToSights := make(map[FeededService][]TrainSight)
	for _, possibleTrip := range possibleTrips {
		possibleTrainSight, hasSight, err := f.getPossibleTrainSight(nm, obsPoint, possibleTrip)
		if err != nil {
			return nil, err
		}
		if !hasSight {
			continue //skip if not a sight
		}
		feededService := FeededService{FeedId: possibleTrip.FeedId, ServiceId: possibleTrip.ServiceId}

		//if it is, add to our map
		serviceToSights[feededService] = append(serviceToSights[feededService], possibleTrainSight)
	}
	fmt.Println("Done calculating trips!")

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
				realTrainSight.updateInnerDates()
				realTrainSights = append(realTrainSights, realTrainSight)
			}
		}
	}

	sort.Slice(realTrainSights, func(i, j int) bool {
		return realTrainSights[i].Timestamp.Before(realTrainSights[j].Timestamp)
	})

	return realTrainSights, nil
}
