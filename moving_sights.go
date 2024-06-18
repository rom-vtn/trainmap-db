package trainmapdb

import (
	"sort"
	"time"
)

func (f *Fetcher) GetSightsFromTripKey(feedId string, tripId string, date Date) ([]RealMovingTrainSight, error) {
	trip, err := f.GetTrip(feedId, tripId)
	if err != nil {
		return nil, err
	}
	return f.GetSightsFromTrip(trip, date)
}

// gets the sights that are visible while riding the given trip on a given date
// NOTE: does not check if the trip is actually running on that day
func (f *Fetcher) GetSightsFromTrip(trip Trip, date Date) ([]RealMovingTrainSight, error) {
	//first get the trips in the interval we want
	const gracePeriod time.Duration = 5 * time.Minute
	firstSt := trip.StopTimes[0]
	lastSt := trip.StopTimes[len(trip.StopTimes)-1]
	overlappingTrips, err := f.GetTripsInsidePointInterval(firstSt.Stop.GetPoint(), lastSt.Stop.GetPoint())
	if err != nil {
		return nil, err
	}

	//get service days to check later when they run
	serviceDays, err := f.GetServicesOnDate(time.Time(date))
	if err != nil {
		return nil, err
	}
	dateToServices := make(map[time.Time][]FeededService)
	for _, serviceDay := range serviceDays {
		date := serviceDay.Date
		dateToServices[date] = append(dateToServices[date], serviceDay.GetFeededService())
	}

	serviceToSights := make(map[FeededService][]MovingTrainSight, 0)
	//check possible sight with every trip
	for _, possibleTrip := range overlappingTrips {
		possibleSight, hasPossibleSight, err := f.getPossibleMovingSight(trip, possibleTrip)
		if err != nil {
			return nil, err
		}
		if !hasPossibleSight {
			continue
		}
		feededService := FeededService{FeedId: possibleTrip.FeedId, ServiceId: possibleTrip.ServiceId}
		serviceToSights[feededService] = append(serviceToSights[feededService], possibleSight)
	}

	realMovingTrainSights := make([]RealMovingTrainSight, 0)
	for date, feededServices := range dateToServices {
		for _, feededService := range feededServices {
			for _, mts := range serviceToSights[feededService] {
				sightTime := mts.PassingInterPoint.Time
				tripDepTime := mts.Trip.StopTimes[0].DepartureTime
				tripArrTime := mts.Trip.StopTimes[len(mts.Trip.StopTimes)-1].ArrivalTime
				if tripDepTime.Add(-gracePeriod).After(sightTime) || tripArrTime.Add(gracePeriod).Before(sightTime) {
					continue
				}
				duration := mts.PassingInterPoint.Time.Sub(time.Unix(0, 0))
				rmts := RealMovingTrainSight{
					MovingTrainSight: mts,
					Date:             date,
					Timestamp:        date.Add(duration),
				}
				rmts.updateInnerDates()
				realMovingTrainSights = append(realMovingTrainSights, rmts)
			}
		}
	}

	sort.Slice(realMovingTrainSights, func(i, j int) bool {
		return realMovingTrainSights[i].Timestamp.Before(realMovingTrainSights[j].Timestamp)
	})

	return realMovingTrainSights, nil
}

type InterpolationPoint struct {
	Position Point
	Time     time.Time
}

func (trip Trip) getPositionAt(time time.Time) Point {
	var stBefore StopTime
	for i, stopTime := range trip.StopTimes {
		if time.Before(*stopTime.ArrivalTime) {
			if i == 0 {
				return stopTime.Stop.GetPoint()
			}
			// do the whole linear interpolation math
			totalTime := stopTime.ArrivalTime.Sub(*stBefore.DepartureTime)
			partialTime := time.Sub(*stBefore.DepartureTime)
			proportion := float64(partialTime / totalTime)
			startPoint := stBefore.Stop.GetPoint()
			endPoint := stopTime.Stop.GetPoint()
			partialLat := proportion * (endPoint.Lat - startPoint.Lat)
			partialLon := proportion * (endPoint.Lon - startPoint.Lon)
			return Point{Lat: startPoint.Lat + partialLat, Lon: startPoint.Lon + partialLon}
		}
		if time.Before(*stopTime.DepartureTime) {
			return stopTime.Stop.GetPoint()
		}
		stBefore = stopTime
	}
	return stBefore.Stop.GetPoint()
}

func (ip InterpolationPoint) getRelativePointTo(other InterpolationPoint) InterpolationPoint {
	relativePt := Point{Lat: other.Position.Lat - ip.Position.Lat, Lon: other.Position.Lon - ip.Position.Lon}
	return InterpolationPoint{Position: relativePt, Time: ip.Time}
}

func (ip InterpolationPoint) getHalfwayPointWith(other InterpolationPoint) InterpolationPoint {
	midPoint := Point{Lat: (other.Position.Lat + ip.Position.Lat) / 2, Lon: (other.Position.Lon + ip.Position.Lon) / 2}
	halfDuration := other.Time.Sub(ip.Time) / time.Duration(2)
	midTimePoint := ip.Time.Add(halfDuration)
	return InterpolationPoint{Position: midPoint, Time: midTimePoint}
}

func (ip InterpolationPoint) getClosestPointWith(other InterpolationPoint, precisionDepth int) InterpolationPoint {
	halfwayInterPoint := ip.getHalfwayPointWith(other)
	if precisionDepth <= 0 {
		return halfwayInterPoint
	}
	thisDist := ip.getAbsDistSquared()
	otherDist := other.getAbsDistSquared()
	if thisDist < otherDist {
		return ip.getClosestPointWith(halfwayInterPoint, precisionDepth-1)
	}
	return halfwayInterPoint.getClosestPointWith(other, precisionDepth-1)
}

func (ip InterpolationPoint) getAbsDistSquared() float64 {
	return ip.Position.Lat*ip.Position.Lat + ip.Position.Lon*ip.Position.Lon
}

type MovingTrainSight struct {
	ServiceId         string             `json:"service_id"`
	TripId            string             `json:"trip_id"`
	FeedId            string             `json:"feed_id"`
	Feed              *Feed              `json:"feed"`
	FirstSt           StopTime           `json:"first_st"`
	LastSt            StopTime           `json:"last_st"`
	Trip              Trip               `json:"trip"`
	RouteName         string             `json:"route_name"`
	PassingInterPoint InterpolationPoint `json:"passing_interpolation_point"`
	Distance          float64            `json:"distance_km"` //distance in kilometers
}

type RealMovingTrainSight struct {
	MovingTrainSight MovingTrainSight `json:"sight"`
	Timestamp        time.Time        `json:"timestamp"`
	Date             time.Time        `json:"date"`
}

func (rmts *RealMovingTrainSight) updateInnerDates() {
	rmts.MovingTrainSight.FirstSt.updateDate(rmts.Date)
	rmts.MovingTrainSight.LastSt.updateDate(rmts.Date)
	for i := range rmts.MovingTrainSight.Trip.StopTimes {
		rmts.MovingTrainSight.Trip.StopTimes[i].updateDate(rmts.Date)
	}
}

func (f Fetcher) getPossibleMovingSight(referenceTrip Trip, possibleTrip Trip) (MovingTrainSight, bool, error) {
	const DEFAULT_PRECISION_DEPTH int = 10
	const MOVING_KM_THRESHOLD = 15 //TODO adjust
	var tripInterpolationPoints, referenceInterpolationPoints []InterpolationPoint
	for _, stopTime := range referenceTrip.StopTimes {
		//add arrival and departure interpolation points for each of our reference stoptimes
		refArrivalInterPoint := InterpolationPoint{Position: stopTime.Stop.GetPoint(), Time: *stopTime.ArrivalTime}
		refDepartureInterPoint := InterpolationPoint{Position: stopTime.Stop.GetPoint(), Time: *stopTime.DepartureTime}
		tripArrivalInterPoint := InterpolationPoint{Position: possibleTrip.getPositionAt(*stopTime.ArrivalTime), Time: *stopTime.ArrivalTime}
		tripDepartureInterPoint := InterpolationPoint{Position: possibleTrip.getPositionAt(*stopTime.DepartureTime), Time: *stopTime.DepartureTime}
		referenceInterpolationPoints = append(referenceInterpolationPoints, refArrivalInterPoint, refDepartureInterPoint)
		tripInterpolationPoints = append(tripInterpolationPoints, tripArrivalInterPoint, tripDepartureInterPoint)
	}
	var previousRelativeInterPoint, lowestInterPoint InterpolationPoint
	var currentLowestDistSquared float64
	var lowestInterPointIsAtEndOfTrip bool
	for i := range tripInterpolationPoints {
		//compare distances for each stoptime
		currentRelativeInterPoint := referenceInterpolationPoints[i].getRelativePointTo(tripInterpolationPoints[i])
		if i != 0 {
			closestPoint := previousRelativeInterPoint.getClosestPointWith(currentRelativeInterPoint, DEFAULT_PRECISION_DEPTH)
			currentDistSquared := closestPoint.getAbsDistSquared()
			if currentLowestDistSquared == 0.0 || currentDistSquared < currentLowestDistSquared {
				//check if ref trip has reached destination (if not moving in an hour)
				ONE_HOUR := time.Hour
				//moving sights have priority over stationary ones, even though stationary sights may be closer
				possibleTripIsAtEnd := possibleTrip.getPositionAt(closestPoint.Time) == possibleTrip.getPositionAt(closestPoint.Time.Add(ONE_HOUR))
				if lowestInterPointIsAtEndOfTrip || !possibleTripIsAtEnd {
					currentLowestDistSquared = currentDistSquared
					lowestInterPoint = closestPoint
					lowestInterPointIsAtEndOfTrip = possibleTripIsAtEnd
				}
			}
		}
		previousRelativeInterPoint = currentRelativeInterPoint
	}
	distanceInKm := lowestInterPoint.Position.getDistTo(Point{})
	if distanceInKm > MOVING_KM_THRESHOLD {
		return MovingTrainSight{}, false, nil
	}

	ourPoint := referenceTrip.getPositionAt(lowestInterPoint.Time)
	absPoint := Point{Lat: ourPoint.Lat + lowestInterPoint.Position.Lat, Lon: ourPoint.Lon + lowestInterPoint.Position.Lon}
	absInterPoint := InterpolationPoint{Position: absPoint, Time: lowestInterPoint.Time}

	mts := MovingTrainSight{
		ServiceId:         possibleTrip.ServiceId,
		TripId:            possibleTrip.TripId,
		FeedId:            possibleTrip.FeedId,
		Feed:              possibleTrip.Feed,
		FirstSt:           possibleTrip.StopTimes[0],
		LastSt:            possibleTrip.StopTimes[len(possibleTrip.StopTimes)-1],
		Trip:              possibleTrip,
		RouteName:         possibleTrip.Route.RouteShortName,
		PassingInterPoint: absInterPoint,
		Distance:          distanceInKm,
	}
	return mts, true, nil
}
