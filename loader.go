package trainmapdb

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jszwec/csvutil"
	"gorm.io/gorm"
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
	//remove BOM from file if there's one
	if len(content) > 3 &&
		content[0] == 0xef &&
		content[1] == 0xbb &&
		content[2] == 0xbf {
		content = content[3:]
	}

	tr := trimReader{csv.NewReader(bytes.NewReader(content))}
	dec, err := csvutil.NewDecoder(&tr)
	if err != nil {
		return err
	}
	err = dec.Decode(&output)
	if err != nil {
		return err
	}
	return nil
}

type trimReader struct{ csvutil.Reader }

func (tr *trimReader) Read() ([]string, error) {
	content, err := tr.Reader.Read()
	if err != nil {
		return nil, err
	}
	for i, v := range content {
		content[i] = strings.TrimSpace(v)
	}
	return content, nil
}

func addToDB[T any](scdb syncCompatibleDB, input []T) error {
	scdb.wgIncrement()
	go func(scdb syncCompatibleDB, input []T) {
		db := scdb.getDB()
		defer scdb.wgDone()
		defer scdb.freeMutex()
		scdb.takeMutex()
		// err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(input).Error
		err := db.Create(input).Error
		if err != nil {
			panic(fmt.Errorf("could not insert to DB: %s", err.Error()))
		}
	}(scdb, input)
	return nil
}

var maxCsvBytes int
var usedCsvBytes int
var csvBytesMutex sync.Mutex

func readCsv[T any](zipFile *zip.Reader, csvFileName string) ([]T, error) {
	file, err := zipFile.Open(csvFileName)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	file.Close()
	if err != nil {
		return nil, err
	}
	currentCsvBytes := int(stat.Size())

	csvBytesMutex.Lock()
	maxCsvBytes = max(maxCsvBytes, currentCsvBytes)
	fmt.Printf("maxCsvBytes: %v\n", maxCsvBytes)
	for usedCsvBytes+currentCsvBytes > maxCsvBytes {
		csvBytesMutex.Unlock()
		println("Too much memory!")
		time.Sleep(100 * time.Millisecond)
		csvBytesMutex.Lock()
	}
	usedCsvBytes += currentCsvBytes
	defer func() {
		csvBytesMutex.Lock()
		defer csvBytesMutex.Unlock()
		usedCsvBytes -= currentCsvBytes
	}()
	csvBytesMutex.Unlock()
	content := []T{}
	err = unmarshalCsv(zipFile, csvFileName, &content)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func parseCalendarDates(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string, validServiceIds map[string]bool) ([]CalendarDate, error) {
	calendarDates, err := readCsv[CalendarDate](zipFile, "calendar_dates.txt")
	if err != nil {
		//NOTE: not a reason to forward the error, GTFS spec allows for no calendar dates
		return []CalendarDate{}, nil
	}
	validCalendarDates := make([]CalendarDate, 0, len(calendarDates))
	for i := range calendarDates {
		if !validServiceIds[calendarDates[i].ServiceId] {
			continue
		}
		calendarDates[i].FeedId = feedId
		date, err := time.Parse("20060102", calendarDates[i].CsvDate)
		if err != nil {
			return nil, err
		}
		calendarDates[i].Date = date
		validCalendarDates = append(validCalendarDates, calendarDates[i])
	}
	return validCalendarDates, addToDB(scdb, validCalendarDates)
}

func parseCalendar(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string, validService map[string]bool) ([]Calendar, error) {
	calendars, err := readCsv[Calendar](zipFile, "calendar.txt")
	if err != nil {
		//NOTE: not a reason to forward the error, GTFS spec allows for no calendars
		return []Calendar{}, nil
	}
	validCalendars := make([]Calendar, 0, len(calendars))
	for i := range calendars {
		if !validService[calendars[i].ServiceId] {
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
		validCalendars = append(validCalendars, calendars[i])
	}
	return validCalendars, addToDB(scdb, validCalendars)
}

func parseStops(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string, validStopIds map[string]bool) error {
	stops, err := readCsv[Stop](zipFile, "stops.txt")
	if err != nil {
		return err
	}
	validStops := make([]Stop, 0, len(stops))
	for i := range stops {
		// filter got removed to make sure parent stops with no stoptimes get included
		// TODO maybe find a better solution??

		// if !validStopIds[stops[i].StopId] {
		// 	continue
		// }

		stops[i].FeedId = feedId
		//go around the foreign key constraint
		if stops[i].CsvParentStationId != "" {
			stops[i].ParentStationId = &stops[i].CsvParentStationId
		}
		err = stops[i].parseLocation()
		if err != nil {
			return err
		}
		validStops = append(validStops, stops[i])
	}
	return addToDB(scdb, validStops)
}

// returns (validTripIds, validServiceIds, err)
func parseTrips(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string, validRouteIds map[string]bool) (validTripIds map[string]bool, validServiceIds map[string]bool, err error) {
	trips, err := readCsv[Trip](zipFile, "trips.txt")
	if err != nil {
		return nil, nil, err
	}
	validTripIds = make(map[string]bool)
	validServiceIds = make(map[string]bool)
	validTrips := make([]Trip, 0, len(trips))
	for i := range trips {
		if _, ok := validRouteIds[trips[i].RefRouteId]; !ok {
			continue
		}
		trips[i].FeedId = feedId
		validTrips = append(validTrips, trips[i])
		validTripIds[trips[i].TripId] = true
		validServiceIds[trips[i].RefServiceId] = true
	}
	return validTripIds, validServiceIds, addToDB(scdb, validTrips)
}

func parseStopTimes(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string, validTripIds map[string]bool) (map[string]bool, error) {
	stopTimes, err := readCsv[StopTime](zipFile, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	validStopTimes := make([]StopTime, 0, len(stopTimes))
	validStopIds := make(map[string]bool)
	for i := range stopTimes {
		if _, ok := validTripIds[stopTimes[i].TripId]; !ok {
			continue
		}
		stopTimes[i].FeedId = feedId
		err = stopTimes[i].convertTimes()
		if err != nil {
			return nil, err
		}
		validStopTimes = append(validStopTimes, stopTimes[i])
		validStopIds[stopTimes[i].StopId] = true
	}
	return validStopIds, addToDB(scdb, validStopTimes)
}

// Convert extended GTFS route types into simple types.
// Used for feeds like Switzerland and Sweden.
// See https://developers.google.com/transit/gtfs/reference/extended-route-types for detail
func simplifyRouteType(rt RouteType) RouteType {
	category := rt / 100 //strip the 2 last digits

	//maps extended route type categories to standard GTFS route types
	var mapping = map[RouteType]RouteType{
		1:  2,  //rail
		2:  3,  //bus
		4:  1,  //subway
		7:  3,  //bus
		8:  11, //trolleybus
		9:  0,  //tram
		10: 4,  //water transit (ferry?)
		// 11 = air transit
		12: 4, //ferry
		13: 6, //aerial lift
		14: 7, //funicular
		// 15 = taxi
		// 17 = miscellaneous
	}
	newIndex, ok := mapping[category]
	if ok {
		return newIndex
	}
	return rt //return itself
}

func parseRoutes(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string) (map[string]bool, error) {
	routes, err := readCsv[Route](zipFile, "routes.txt")
	if err != nil {
		return nil, err
	}
	validRouteIds := make(map[string]bool)
	validRoutes := make([]Route, 0, len(routes))
	for i := range routes {
		routes[i].FeedId = feedId
		routes[i].RouteType = simplifyRouteType(routes[i].RouteType)
		if routes[i].RouteType != RouteTypeBus {
			validRoutes = append(validRoutes, routes[i])
			validRouteIds[routes[i].RouteId] = true
		}
	}
	return validRouteIds, addToDB(scdb, validRoutes)
}

func parseAgencies(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string) error {
	agencies, err := readCsv[Agency](zipFile, "agency.txt")
	if err != nil {
		return err
	}
	for i := range agencies {
		agencies[i].FeedId = feedId
	}
	return addToDB(scdb, agencies)
}

func parseFeed(zipFile *zip.Reader, scdb syncCompatibleDB, feedId string, displayName string) error {
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
	return addToDB(scdb, feeds)
}

// calculate service days based on calendars and calendarDates to make lookups easier
func calculateServiceDays(scdb syncCompatibleDB, calendars []Calendar, calendarDates []CalendarDate) error {
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
		//save negative exceptions to compare with calendars
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
			_, hasNegativeServiceException := serviceDayToCalendarDate[serviceDay]
			isServiceRunning := false
			if !hasNegativeServiceException {
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

	err := addToDB(scdb, serviceDays)
	return err
}

// a LoaderConfig represents a config used to load GTFS feeds into a DB.
type LoaderConfig struct {
	DatabasePath string              `json:"db_path"`
	Contents     []LoaderConfigEntry `json:"contents"`
}

// a LoaderConfigEntry contains info about a specific GTFS feed and how it should be loaded.
type LoaderConfigEntry struct {
	Active             bool   `json:"active"`
	FeedURL            string `json:"feed_url"`
	FetchIntervalHours *uint  `json:"fetch_interval_hours"` //0 = always fetch, null = always rely on local file
	DatabaseFileName   string `json:"db_filename"`
	DisplayName        string `json:"display_name"`
}

func migrate(db *gorm.DB, disableForeignKeyConstraints bool) error {
	fkOriginalSettings := db.Config.DisableForeignKeyConstraintWhenMigrating
	db.Config.DisableForeignKeyConstraintWhenMigrating = disableForeignKeyConstraints
	err := db.AutoMigrate(&Feed{}, &Agency{}, &Calendar{}, &CalendarDate{}, &ServiceDay{}, &Stop{}, &Route{}, &Trip{}, &StopTime{})
	db.Config.DisableForeignKeyConstraintWhenMigrating = fkOriginalSettings
	return err
}

// LoadDatabase builds a database from the given LoaderConfig into the given file.
func (f Fetcher) LoadDatabase(config LoaderConfig) error {
	stat, err := os.Stat(config.DatabasePath)
	hasData := true
	if errors.Is(err, os.ErrNotExist) {
		hasData = false
		err = nil //don't consider the file not existing as an error
	}
	if err != nil {
		return err //handle all other errors
	}
	if hasData { //check if not an empty file
		hasData = stat.Size() > 0
	}
	if hasData {
		return fmt.Errorf("database file %s already exists, please remove it first", config.DatabasePath)
	}

	//migrate schema
	db := f.db

	err = migrate(db, !f.useMutex)
	if err != nil {
		return fmt.Errorf("error when automigrating: %s", err.Error())
	}

	var processingWg sync.WaitGroup
	var scdb syncCompatibleDB
	if f.useMutex {
		scdb = &mutexedDB{db: db}
	} else {
		scdb = &unmutexedDB{db: db}
	}

	// load all the data we got
	for feedIdInt, configEntry := range config.Contents {
		if !configEntry.Active {
			continue
		}
		feedFileName := configEntry.DatabaseFileName
		feedId := fmt.Sprintf("%d", feedIdInt+1) //add 1 to not have an empty PK field
		processingWg.Add(1)
		go func(feedFileName string, feedId string, scdb syncCompatibleDB, configEntry LoaderConfigEntry) {
			defer processingWg.Done()
			err := processFeed(feedId, scdb, configEntry)
			if err != nil {
				panic(fmt.Errorf("[%s] Error while parsing feed %s : %s", configEntry.DisplayName, feedFileName, err.Error()))
			}
			log.Default().Printf("[%s] Done with processing!\n", configEntry.DisplayName)
		}(feedFileName, feedId, scdb, configEntry)
	}

	log.Default().Println("Waiting for file parsing to be done...")
	processingWg.Wait()

	log.Default().Println("Waiting for all the entries to be written to the DB before running optimization SQL...")
	scdb.wgWait()

	if !f.useMutex {
		log.Default().Println("Adding FK contraints...")
		err = migrate(db, false)
		if err != nil {
			return fmt.Errorf("error when adding foreign keys: %s", err.Error())
		}
	}

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

func downloadFeed(feedURL string) ([]byte, error) {
	// first download the feed
	resp, err := http.DefaultClient.Get(feedURL)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("expected http 200 from %s, got %d instead", feedURL, resp.StatusCode)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func shouldDownload(configEntry LoaderConfigEntry) (bool, error) {
	//never download if set to null
	if configEntry.FetchIntervalHours == nil {
		return false, nil
	}
	fileName := configEntry.DatabaseFileName
	stat, err := os.Stat(fileName)
	//if no cache, then download
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if stat.IsDir() {
		return false, fmt.Errorf("%s: expected a file, but path points to a directory", fileName)
	}
	//check how long since last modified and check interval
	lastModifiedDuration := time.Since(stat.ModTime())
	lastModifiedThreshold := time.Duration(*configEntry.FetchIntervalHours) * time.Hour
	return lastModifiedDuration > lastModifiedThreshold, nil
}

func processFeed(feedId string, scdb syncCompatibleDB, configEntry LoaderConfigEntry) error {
	feedFileName := configEntry.DatabaseFileName
	feedURL := configEntry.FeedURL

	//check if feed should be downloaded, if so download, otherwise get from local file
	download, err := shouldDownload(configEntry)
	if err != nil {
		return err
	}
	var content []byte
	if download {
		log.Default().Printf("[%s] Starting download of %s \n", configEntry.DisplayName, feedFileName)
		content, err = downloadFeed(feedURL)
		if err != nil {
			return err
		}
		log.Default().Printf("[%s] Done downloading, caching to %s...", configEntry.DisplayName, feedFileName)
		//TODO maybe 0644 isn't really ideal but who cares
		err = os.WriteFile(feedFileName, content, 0644)
		if err != nil {
			return err
		}
		log.Default().Printf("[%s] Done caching, staring parsing...\n", configEntry.DisplayName)
	} else {
		log.Default().Printf("[%s] Using saved cached file at %s...\n", configEntry.DisplayName, feedFileName)
		content, err = os.ReadFile(feedFileName)
		if err != nil {
			return err
		}
	}

	//then open the file
	zipFile, err := zip.NewReader(bytes.NewReader(content), (int64)(len(content)))
	if err != nil {
		return err
	}
	validRouteIds, err := parseRoutes(zipFile, scdb, feedId)
	if err != nil {
		return err
	}
	validTripIds, validServiceIds, err := parseTrips(zipFile, scdb, feedId, validRouteIds)
	if err != nil {
		return err
	}
	calendarDates, err := parseCalendarDates(zipFile, scdb, feedId, validServiceIds)
	if err != nil {
		return err
	}
	calendar, err := parseCalendar(zipFile, scdb, feedId, validServiceIds)
	if err != nil {
		return err
	}
	err = calculateServiceDays(scdb, calendar, calendarDates)
	if err != nil {
		return err
	}
	validStopIds, err := parseStopTimes(zipFile, scdb, feedId, validTripIds)
	if err != nil {
		return err
	}
	err = parseStops(zipFile, scdb, feedId, validStopIds)
	if err != nil {
		return err
	}
	err = parseAgencies(zipFile, scdb, feedId)
	if err != nil {
		return err
	}
	err = parseFeed(zipFile, scdb, feedId, configEntry.DisplayName)
	if err != nil {
		return err
	}
	return nil
}
