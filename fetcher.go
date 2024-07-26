package trainmapdb

import (
	"fmt"
	"math"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FetcherConfig struct {
	OutOfBoundsGraceAbsolute        float64
	OutOfBoundsGracePercentage      float64
	BearingMinThreshold             Bearing //anything below this will be not be considered as much
	BearingMaxThreshold             Bearing //anything above this will be more favorably considered
	DatabaseOutOfBoundsGraceDegrees float64
	CloseHeavyRailStationThreshold  float64 //kilometers
	CloseTramStationThreshold       float64 //kilometers
}

func NewDefaultConfig() FetcherConfig {
	return FetcherConfig{
		OutOfBoundsGraceAbsolute:        0.009 * 15, //about 15km in degrees
		OutOfBoundsGracePercentage:      0.10,
		BearingMinThreshold:             Bearing(2 * math.Pi / 6), //60deg
		BearingMaxThreshold:             Bearing(5 * math.Pi / 6), //150deg
		DatabaseOutOfBoundsGraceDegrees: 0.009 * 2,                //used in SQL for trip bounding box
		CloseHeavyRailStationThreshold:  0.6,
		CloseTramStationThreshold:       0.2,
	}
}

type Fetcher struct {
	db     *gorm.DB
	Config FetcherConfig
}

func NewFetcher(dbFileName string, config *FetcherConfig) (*Fetcher, error) {
	var fetcherConfig FetcherConfig
	if config != nil {
		fetcherConfig = *config
	} else {
		fetcherConfig = NewDefaultConfig()
	}
	db, err := gorm.Open(sqlite.Open(dbFileName), &gorm.Config{
		CreateBatchSize: 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("error while opening DB: %s", err.Error())
	}
	return &Fetcher{db: db, Config: fetcherConfig}, nil
}

func (f Fetcher) GetAllTrips() ([]Trip, error) {
	var trips, tripBatch []Trip
	const BATCH_SIZE int = 1000
	batchIndex := 0
	for {
		err := f.db.Offset(BATCH_SIZE * batchIndex).Limit(BATCH_SIZE).Preload(clause.Associations).Preload("StopTimes.Stop").Find(&tripBatch).Error
		if err != nil {
			return nil, err
		}
		if len(tripBatch) == 0 {
			break
		}
		trips = append(trips, tripBatch...)
		batchIndex++
	}
	return trips, nil
}

func (f Fetcher) GetAllStops() ([]Stop, error) {
	var stops []Stop
	err := f.db.Model(&Stop{}).Preload(clause.Associations).Find(&stops).Error
	return stops, err
}

type FeededService struct {
	FeedId    string
	ServiceId string
}

func (f Fetcher) GetServicesOnDate(date time.Time) ([]ServiceDay, error) {
	//NOTE: SQL "BETWEEN" is inclusive on both sides
	return f.GetServicesBetweenDates(date, date)
}

func (f Fetcher) GetServicesBetweenDates(startDate time.Time, endDate time.Time) ([]ServiceDay, error) {
	var serviceDays []ServiceDay
	err := f.db.Model(&ServiceDay{}).Where("Date BETWEEN ? AND ?", startDate, endDate).Find(&serviceDays).Error
	if err != nil {
		return nil, err
	}

	return serviceDays, nil
}

func (f Fetcher) GetFeededServiceIdTrips(feededService FeededService) ([]Trip, error) {
	var trips []Trip
	err := f.db.Where("FeedId = ?", feededService.FeedId).Where("ServiceId = ?", feededService.ServiceId).Find(&trips).Error
	if err != nil {
		return nil, err
	}
	return trips, nil
}

func (f Fetcher) GetTrip(feedId string, tripId string) (Trip, error) {
	trip := Trip{FeedId: feedId, TripId: tripId}
	err := f.db.Model(&Trip{}).Preload(clause.Associations).Preload("Route").Preload("StopTimes.Stop").Preload("StopTimes.Trip").Where(&trip).First(&trip).Error
	if err != nil {
		return trip, err
	}
	return trip, err
}

func (f Fetcher) GetRoute(feedId string, routeId string) (Route, error) {
	route := Route{FeedId: feedId, RouteId: routeId}
	err := f.db.Model(&Route{}).Preload(clause.Associations).Preload("Trips.StopTimes.Stop").Where(&route).First(&route).Error
	return route, err
}

func (f Fetcher) GetFeed(feedId string) (Feed, error) {
	feed := Feed{FeedId: feedId}
	err := f.db.Where(&feed).First(&feed).Error
	return feed, err
}

func (f Fetcher) GetTripsContaining(pt Point) ([]Trip, error) {
	return f.GetTripsInsidePointInterval(pt, pt)
}

func pointsToBoundingBox(pt1, pt2 Point) (minLat, minLon, maxLat, maxLon float64) {
	minLat = min(pt1.Lat, pt2.Lat)
	maxLat = max(pt1.Lat, pt2.Lat)
	minLon = min(pt1.Lon, pt2.Lon)
	maxLon = max(pt1.Lon, pt2.Lon)
	return minLat, maxLat, minLon, maxLon
}

func (f Fetcher) GetTripsInsidePointInterval(pt1 Point, pt2 Point) ([]Trip, error) {
	return f.GetTripsWithIntersection(pointsToBoundingBox(pt1, pt2))
}

func (f Fetcher) GetTripsWithIntersection(minLat float64, maxLat float64, minLon float64, maxLon float64) ([]Trip, error) {
	var trips, tripBatch []Trip
	const batchSize int = 1000
	batchIndex := 0
	grace := f.Config.DatabaseOutOfBoundsGraceDegrees
	for {
		err := f.db.Offset(batchSize*batchIndex).Limit(batchSize).
			Where("(max_lat >= ?) AND (min_lat <= ?) AND (max_lon >= ?) AND (min_lon <= ?)",
				minLat-grace,
				maxLat+grace,
				minLon-grace,
				maxLon+grace).
			Preload(clause.Associations).Preload("StopTimes.Stop").Find(&tripBatch).Error
		if err != nil {
			return nil, err
		}
		if len(tripBatch) == 0 {
			break
		}
		trips = append(trips, tripBatch...)
		batchIndex++
	}
	return trips, nil
}

func (f Fetcher) GetStop(feedId string, stopId string) (Stop, error) {
	stop := Stop{FeedId: feedId, StopId: stopId}
	err := f.db.Preload(clause.Associations).Preload("StopTimes.Trip").Where(&stop).First(&stop).Error
	return stop, err
}

func (f Fetcher) GetFeeds() ([]Feed, error) {
	var feeds []Feed
	err := f.db.Where(&feeds).Find(&feeds).Error
	return feeds, err
}

func (f Fetcher) GetStopTimesAtStop(feedId string, stopId string) ([]StopTime, error) {
	var stopTimes []StopTime
	err := f.db.Where(&StopTime{FeedId: feedId, StopId: stopId}).Find(&stopTimes).Error
	return stopTimes, err
}

func (f Fetcher) GetAllAgencies() ([]Agency, error) {
	var agencies []Agency
	err := f.db.Where(&agencies).Find(&agencies).Error
	if err != nil {
		return nil, err
	}
	return agencies, nil

}
