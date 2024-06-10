package trainmapdb

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jszwec/csvutil"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func unmarshalCsv[T any](zipFile *zip.Reader, csvFileName string, output *[]T) error {
	csvFile, err := zipFile.Open(csvFileName)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	content, err := io.ReadAll(csvFile)
	if err != nil {
		return err
	}

	return csvutil.Unmarshal(content, output)
}

func addToDB[T any](mutexedDb *MutexedDB, input []T) error {
	mutexedDb.wg.Add(1)
	go func(mutexedDB *MutexedDB, input []T) {
		dbMutex := &mutexedDb.mutex
		db := mutexedDb.db
		defer mutexedDb.wg.Done()
		defer dbMutex.Unlock()
		dbMutex.Lock()
		err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(input).Error
		if err != nil {
			panic(fmt.Errorf("could not insert to DB: %s", err.Error()))
		}
	}(mutexedDb, input)
	return nil
}

func readCsv[T any](zipFile *zip.Reader, csvFileName string) ([]T, error) {
	content := []T{}
	err := unmarshalCsv(zipFile, csvFileName, &content)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func parseCalendarDates(zipFile *zip.Reader, db *MutexedDB, feedId string, nonBusServiceIds map[string]bool) ([]CalendarDate, error) {
	calendarDates, err := readCsv[CalendarDate](zipFile, "calendar_dates.txt")
	if err != nil {
		return []CalendarDate{}, nil
	}
	nonBusCalendarDates := make([]CalendarDate, 0, len(calendarDates))
	for i := range calendarDates {
		if !nonBusServiceIds[calendarDates[i].ServiceId] {
			continue
		}
		calendarDates[i].FeedId = feedId
		date, err := time.Parse("20060102", calendarDates[i].CsvDate)
		if err != nil {
			return nil, err
		}
		calendarDates[i].Date = date
		nonBusCalendarDates = append(nonBusCalendarDates, calendarDates[i])
	}
	return calendarDates, addToDB(db, nonBusCalendarDates)
}

func parseCalendar(zipFile *zip.Reader, db *MutexedDB, feedId string, nonBusServiceIds map[string]bool) ([]Calendar, error) {
	calendars, err := readCsv[Calendar](zipFile, "calendar.txt")
	if err != nil {
		return []Calendar{}, nil
	}
	nonBusCalendars := make([]Calendar, 0, len(calendars))
	for i := range calendars {
		if !nonBusServiceIds[calendars[i].ServiceId] {
			continue
		}
		//set feed
		calendars[i].FeedId = feedId
		//set start date
		calendars[i].StartDate, err = time.Parse("20060102", calendars[i].CsvStartDate)
		if err != nil {
			return nil, err
		}
		//set end date
		calendars[i].EndDate, err = time.Parse("20060102", calendars[i].CsvEndDate)
		if err != nil {
			return nil, err
		}
		nonBusCalendars = append(nonBusCalendars, calendars[i])
	}
	return nonBusCalendars, addToDB(db, nonBusCalendars)
}

func parseStops(zipFile *zip.Reader, db *MutexedDB, feedId string, nonBusStopIds map[string]bool) error {
	stops, err := readCsv[Stop](zipFile, "stops.txt")
	if err != nil {
		return err
	}
	nonBusStops := make([]Stop, 0, len(stops))
	for i := range stops {
		if !nonBusStopIds[stops[i].StopId] {
			continue
		}
		stops[i].FeedId = feedId
		err = stops[i].parseLocation()
		if err != nil {
			return err
		}
		nonBusStops = append(nonBusStops, stops[i])
	}
	return addToDB(db, nonBusStops)
}

// returns (nonBusTripIds, nonBusServiceIds, err)
func parseTrips(zipFile *zip.Reader, db *MutexedDB, feedId string, nonBusRouteIds map[string]bool) (validTripIds map[string]bool, validServiceIds map[string]bool, err error) {
	trips, err := readCsv[Trip](zipFile, "trips.txt")
	if err != nil {
		return nil, nil, err
	}
	nonBusTripIds := make(map[string]bool)
	nonBusServiceIds := make(map[string]bool)
	nonBusTrips := make([]Trip, 0, len(trips))
	for i := range trips {
		if _, ok := nonBusRouteIds[trips[i].RouteId]; !ok {
			continue
		}
		trips[i].FeedId = feedId
		nonBusTrips = append(nonBusTrips, trips[i])
		nonBusTripIds[trips[i].TripId] = true
		nonBusServiceIds[trips[i].ServiceId] = true
	}
	return nonBusTripIds, nonBusServiceIds, addToDB(db, nonBusTrips)
}

func parseStopTimes(zipFile *zip.Reader, db *MutexedDB, feedId string, nonBusTripIds map[string]bool) (map[string]bool, error) {
	stopTimes, err := readCsv[StopTime](zipFile, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	nonBusStopTimes := make([]StopTime, 0, len(stopTimes))
	nonBusStopIds := make(map[string]bool)
	for i := range stopTimes {
		if _, ok := nonBusTripIds[stopTimes[i].TripId]; !ok {
			continue
		}
		stopTimes[i].FeedId = feedId
		err = stopTimes[i].convertTimes()
		if err != nil {
			return nil, err
		}
		nonBusStopTimes = append(nonBusStopTimes, stopTimes[i])
		nonBusStopIds[stopTimes[i].StopId] = true
	}
	return nonBusStopIds, addToDB(db, nonBusStopTimes)
}

func parseRoutes(zipFile *zip.Reader, db *MutexedDB, feedId string) (map[string]bool, error) {
	routes, err := readCsv[Route](zipFile, "routes.txt")
	if err != nil {
		return nil, err
	}
	nonBusRouteIds := make(map[string]bool)
	nonBusRoutes := make([]Route, 0, len(routes))
	for i := range routes {
		routes[i].FeedId = feedId
		if routes[i].RouteType != RouteTypeBus {
			nonBusRoutes = append(nonBusRoutes, routes[i])
			nonBusRouteIds[routes[i].RouteId] = true
		}
	}
	return nonBusRouteIds, addToDB(db, nonBusRoutes)
}

func parseAgencies(zipFile *zip.Reader, db *MutexedDB, feedId string) error {
	agencies, err := readCsv[Agency](zipFile, "agency.txt")
	if err != nil {
		return err
	}
	for i := range agencies {
		agencies[i].FeedId = feedId
	}
	return addToDB(db, agencies)
}

func parseFeed(zipFile *zip.Reader, db *MutexedDB, feedId string, displayName string) error {
	//NOTE: errors are possible if no feed_info is given, in this case we just add our own feed info entry
	feeds, _ := readCsv[Feed](zipFile, "feed_info.txt")
	if feeds == nil {
		feeds = append(feeds, Feed{FeedId: feedId})
	}
	for i := range feeds {
		feeds[i].FeedId = feedId
		feeds[i].DisplayName = displayName
	}
	// TODO check that this is how the GTFS spec should really be implemented
	return addToDB(db, feeds)
}

// calculate service days based on calendars and calendarDates to make lookups easier
func calculateServiceDays(db *MutexedDB, calendars []Calendar, calendarDates []CalendarDate) error {
	const ONE_DAY = 24 * time.Hour

	var serviceDays []ServiceDay
	serviceDayToCalendarDate := make(map[ServiceDay]CalendarDate)
	//index service exceptions by FeededService for easier lookups
	for _, calendarDate := range calendarDates {
		serviceDay := ServiceDay{
			FeedId:    calendarDate.FeedId,
			ServiceId: calendarDate.ServiceId,
			Date:      calendarDate.Date,
		}

		//add positive exceptions separately, since they may not refer to any real calendar entry
		if calendarDate.ExceptionType == ExceptionTypeServiceAdded {
			serviceDays = append(serviceDays, serviceDay)
		}
		serviceDayToCalendarDate[serviceDay] = calendarDate
	}

	//then perform actual lookup for regular calendars days only
	for _, calendar := range calendars {
		date := calendar.StartDate
		for date.Before(calendar.EndDate) {
			serviceDay := ServiceDay{
				FeedId:    calendar.FeedId,
				ServiceId: calendar.ServiceId,
				Date:      date,
			}
			_, hasServiceException := serviceDayToCalendarDate[serviceDay]
			isServiceRunning := false
			if !hasServiceException {
				//if no service exception at the date:
				//check weekday for regular schedule
				isServiceRunning = calendar.GetWeekdayStatus(date.Weekday())
			}
			if isServiceRunning {
				serviceDays = append(serviceDays, serviceDay)
			}
			date = date.Add(ONE_DAY)
		}
	}
	//then add service exceptions

	err := addToDB(db, serviceDays)
	return err
}

type LoaderConfig struct {
	DatabasePath string              `json:"db_path"`
	Contents     []LoaderConfigEntry `json:"contents"`
}

type LoaderConfigEntry struct {
	Active           bool   `json:"active"`
	FeedURL          string `json:"feed_url"`
	DatabaseFileName string `json:"db_filename"`
	DisplayName      string `json:"display_name"`
}

type MutexedDB struct {
	db    *gorm.DB
	mutex sync.Mutex
	wg    sync.WaitGroup
}

func (f Fetcher) LoadDatabase(config LoaderConfig) error {
	//migrate schema
	db := f.db
	err := db.AutoMigrate(&Calendar{}, &CalendarDate{}, &Trip{}, &Stop{}, &StopTime{}, &Route{}, &Agency{}, &Feed{}, &ServiceDay{})
	if err != nil {
		return fmt.Errorf("error when automigrating: %s", err.Error())
	}

	var processingWg sync.WaitGroup
	mutexedDB := &MutexedDB{db: db}

	// load all the data we got
	for feedIdInt, configEntry := range config.Contents {
		if !configEntry.Active {
			continue
		}
		feedFileName := configEntry.DatabaseFileName
		feedId := fmt.Sprintf("%d", feedIdInt+1) //add 1 to not have an empty PK field
		processingWg.Add(1)
		go func(feedFileName string, feedId string, mutexedDB *MutexedDB, configEntry LoaderConfigEntry) {
			defer processingWg.Done()
			err := processFeed(feedId, mutexedDB, configEntry)
			if err != nil {
				panic(fmt.Errorf("[%s] Error while parsing feed %s : %s", configEntry.DisplayName, feedFileName, err.Error()))
			}
			log.Default().Printf("[%s] Done with processing!\n", configEntry.DisplayName)
		}(feedFileName, feedId, mutexedDB, configEntry)
	}

	log.Default().Println("Waiting for file parsing to be done...")
	processingWg.Wait()

	log.Default().Println("Waiting for all the entries to be written to the DB before running optimization SQL...")
	mutexedDB.wg.Wait()

	log.Default().Println("Running optimization SQL...")

	//then compile the whole min/lat lon/lat for trips and add the geo index
	//first suppress SLOW SQL warning, this is run only once

	customLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{SlowThreshold: 30 * time.Second})
	session := db.Session(&gorm.Session{Logger: customLogger})
	sqlString := `
		UPDATE trips
		SET
			min_lat = r.min_lat,
			max_lat = r.max_lat,
			min_lon = r.min_lon,
			max_lon = r.max_lon
		FROM (
			SELECT
				st.feed_id as feed_id,
				st.trip_id as trip_id,
				MIN(s.stop_lat) as min_lat,
				MAX(s.stop_lat) as max_lat,
				MIN(s.stop_lon) as min_lon,
				MAX(s.stop_lon) as max_lon
			FROM (stop_times as st JOIN stops as s ON st.stop_id = s.stop_id)
			GROUP BY st.trip_id, st.feed_id
		) as r
		WHERE r.trip_id = trips.trip_id
		AND r.feed_id = trips.feed_id;`
	err = session.Exec(sqlString).Error
	if err != nil {
		return err
	}
	return nil
}

func processFeed(feedId string, mutexedDb *MutexedDB, configEntry LoaderConfigEntry) error {
	feedFileName := configEntry.DatabaseFileName
	feedURL := configEntry.FeedURL

	log.Default().Printf("[%s] Starting download of %s \n", configEntry.DisplayName, feedFileName)

	// first download the feed
	resp, err := http.DefaultClient.Get(feedURL)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("expected http 200 from %s, got %d instead", feedURL, resp.StatusCode)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.Default().Printf("[%s] Done downloading file, starting parsing...\n", configEntry.DisplayName)

	//then open the file
	zipFile, err := zip.NewReader(bytes.NewReader(content), (int64)(len(content)))
	if err != nil {
		return err
	}
	nonBusRouteIds, err := parseRoutes(zipFile, mutexedDb, feedId)
	if err != nil {
		return err
	}
	nonBusTripIds, nonBusServiceIds, err := parseTrips(zipFile, mutexedDb, feedId, nonBusRouteIds)
	if err != nil {
		return err
	}
	calendarDates, err := parseCalendarDates(zipFile, mutexedDb, feedId, nonBusServiceIds)
	if err != nil {
		return err
	}
	calendar, err := parseCalendar(zipFile, mutexedDb, feedId, nonBusServiceIds)
	if err != nil {
		return err
	}
	err = calculateServiceDays(mutexedDb, calendar, calendarDates)
	if err != nil {
		return err
	}
	nonBusStopIds, err := parseStopTimes(zipFile, mutexedDb, feedId, nonBusTripIds)
	if err != nil {
		return err
	}
	err = parseStops(zipFile, mutexedDb, feedId, nonBusStopIds)
	if err != nil {
		return err
	}
	err = parseAgencies(zipFile, mutexedDb, feedId)
	if err != nil {
		return err
	}
	err = parseFeed(zipFile, mutexedDb, feedId, configEntry.DisplayName)
	if err != nil {
		return err
	}
	return nil
}
