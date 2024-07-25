package trainmapdb

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Agency struct {
	FeedId         string `gorm:"primaryKey;index:pk_agency" json:"feed_id"`
	AgencyId       string `gorm:"primaryKey;index:pk_agency" csv:"agency_id" json:"agency_id"`
	AgencyName     string `csv:"agency_name" json:"name"`
	AgencyUrl      string `csv:"agency_url"`
	AgencyTimezone string `csv:"agency_timezone"`
	AgencyLang     string `csv:"agency_lang"`
	//skipping the rest
}

func (Agency) TableName() string {
	return "agency"
}

type LocationType int

const (
	LocationTypePlatform LocationType = iota
	LocationTypeStation
	LocationTypeEntranceExit
	LocationTypeGeneric
	LocationTypeBoardingArea
)

type Stop struct {
	Feed            Feed          `gorm:"foreignKey:FeedId;references:FeedId" json:"feed"`
	FeedId          string        `gorm:"primaryKey;index:pk_stop" json:"feed_id"`
	StopId          string        `gorm:"primaryKey;index:pk_stop" csv:"stop_id" json:"stop_id"`
	StopCode        string        `csv:"stop_code" json:"stop_code"`
	StopName        string        `csv:"stop_name" json:"stop_name"`
	TtsStopName     string        `csv:"tts_stop_name" json:"tts_stop_name"`
	StopDesc        string        `csv:"stop_desc" json:"stop_desc"`
	StopUrl         string        `csv:"stop_url" json:"stop_url"`
	StopLat         float64       `json:"lat"`
	StopLon         float64       `json:"lon"`
	StopLatString   string        `csv:"stop_lat" json:"-"`
	StopLonString   string        `csv:"stop_lon" json:"-"`
	LocationType    *LocationType `csv:"location_type" json:"location_type"` // 0=Stop/platform, 1=Station, 2=Entrance/exit, 3=Generic, 4=Boarding area
	ParentStationId string        `csv:"parent_station" json:"-"`
	ParentStation   *Stop         `gorm:"foreignKey:ParentStationId,FeedId;references:StopId,FeedId" json:"parent_station"`
	ChildStations   []Stop        `gorm:"foreignKey:ParentStationId,FeedId;references:StopId,FeedId" json:"child_stations"`
	StopTimes       []StopTime    `gorm:"foreignKey:FeedId,StopId;references:FeedId,StopId" json:"stop_times"`
}

func (s *Stop) parseLocation() error {
	lat, err := trimAndParseFloat(s.StopLatString)
	if err != nil {
		return err
	}
	lon, err := trimAndParseFloat(s.StopLonString)
	if err != nil {
		return err
	}
	s.StopLat = lat
	s.StopLon = lon
	return nil
}

func trimAndParseFloat(s string) (float64, error) {
	trimmed := strings.Trim(s, " ")
	val, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

type RouteType int

const (
	RouteTypeTram RouteType = iota
	RouteTypeSubway
	RouteTypeHeavyRail
	RouteTypeBus
	//NOTE: might add more transport options
)

func (r RouteType) isRailType() bool {
	return r == RouteTypeTram ||
		r == RouteTypeHeavyRail
}

type Route struct {
	Feed           Feed      `gorm:"foreignKey:FeedId;references:FeedId" json:"feed"`
	FeedId         string    `gorm:"primaryKey;index:pk_route" json:"feed_id"`
	RouteId        string    `gorm:"primaryKey;index:pk_route" csv:"route_id" json:"route_id"`
	RouteShortName string    `csv:"route_short_name" json:"short_name"`
	RouteLongName  string    `csv:"route_long_name" json:"long_name"`
	RouteDesc      string    `csv:"route_desc" json:"description"`
	RouteType      RouteType `csv:"route_type" json:"type"` //Basically: 0=tram,1=subway,2=heavy rail, 3=Bus, other=not rail (see docs for more info)
	RouteColor     string    `csv:"route_color" json:"color"`
	RouteTextColor string    `csv:"route_text_color" json:"text_color"`
	Trips          []Trip    `gorm:"foreignKey:FeedId,RouteId;references:FeedId,RouteId" json:"trips"`
}

type Trip struct {
	FeedId        string         `gorm:"primaryKey;index:pk_trip" json:"feed_id"`
	TripId        string         `gorm:"primaryKey;index:pk_trip" csv:"trip_id" json:"trip_id"`
	Feed          *Feed          `gorm:"foreignKey:FeedId;references:FeedId" json:"feed"`
	RouteId       string         `csv:"route_id" json:"-"`
	Route         *Route         `gorm:"foreignKey:RouteId,FeedId;references:RouteId,FeedId" json:"route"`
	ServiceId     string         `csv:"service_id" json:"service_id"`
	Calendar      Calendar       `json:"calendar" gorm:"foreignKey:FeedId,ServiceId;references:FeedId,ServiceId"`
	CalendarDates []CalendarDate `json:"calendar_dates" gorm:"foreignKey:FeedId,ServiceId;references:FeedId,ServiceId"`
	Headsign      string         `csv:"trip_headsign" json:"headsign"`
	TripShortName string         `csv:"trip_short_name" json:"short_name"`
	StopTimes     []StopTime     `gorm:"foreignKey:FeedId,TripId;references:FeedId,TripId" json:"stop_times"`
	//NOTE: no LongName is speicified in the spec
	//Additional feeds (not part of the gtfs spec but used by our implementation)
	MinLat float64 `gorm:"index:geo_index" json:"-"`
	MaxLat float64 `gorm:"index:geo_index" json:"-"`
	MinLon float64 `gorm:"index:geo_index" json:"-"`
	MaxLon float64 `gorm:"index:geo_index" json:"-"`
}

type ServiceType uint

const (
	ServiceTypeScheduled ServiceType = iota
	ServiceTypeNotPossible
	ServiceTypeMustPhone
	ServiceTypeMustCoordinateWithDriver
)

type StopTime struct {
	FeedId           string      `gorm:"primaryKey;index:pk_stoptime" json:"feed_id"`
	TripId           string      `gorm:"primaryKey;index:pk_stoptime" csv:"trip_id" json:"trip_id"`
	CsvArrivalTime   string      `gorm:"-:all" csv:"arrival_time" json:"-"`   //hh:mm:ss
	CsvDepartureTime string      `gorm:"-:all" csv:"departure_time" json:"-"` //hh:mm:ss
	ArrivalTime      time.Time   `json:"arrival_time"`
	DepartureTime    time.Time   `json:"departure_time"`
	StopId           string      `csv:"stop_id" json:"stop_id"`
	Stop             *Stop       `gorm:"foreignKey:StopId,FeedId;references:StopId,FeedId" json:"stop"`
	Trip             *Trip       `gorm:"foreignKey:TripId,FeedId;references:TripId,FeedId" json:"trip"`
	StopSequence     uint        `gorm:"primaryKey;index:pk_stoptime" csv:"stop_sequence" json:"stop_sequence"`
	StopHeadsign     string      `csv:"stop_headsign" json:"stop_headsign"`
	PickupType       ServiceType `csv:"pickup_type" json:"pickup_type"`
	DropOffType      ServiceType `csv:"pickup_type" json:"dorp_off_type"`
}

func (st *StopTime) updateDate(date time.Time) {
	if !st.ArrivalTime.IsZero() {
		newTime := date.Add(st.ArrivalTime.Sub(time.Unix(0, 0)))
		st.ArrivalTime = newTime
	}
	if !st.DepartureTime.IsZero() {
		newTime := date.Add(st.DepartureTime.Sub(time.Unix(0, 0)))
		st.DepartureTime = newTime
	}
}

// converts time as "hh:mm:ss" to a duration and returns (unix epoch + duration).
// if timeString == "", then returns (time.Time{}, nil)
func convertTime(timeString string) (time.Time, error) {
	//if no info given, return nil/nil
	if timeString == "" {
		return time.Time{}, nil
	}
	//expect hh:mm:ss
	timeSlice := strings.Split(timeString, ":")
	if len(timeSlice) < 3 {
		return time.Time{}, fmt.Errorf("could not split time format properly")
	}
	hours, err := strconv.ParseUint(timeSlice[0], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	minutes, err := strconv.ParseUint(timeSlice[1], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	seconds, err := strconv.ParseUint(timeSlice[2], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	duration := time.Duration(hours*uint64(time.Hour) + minutes*uint64(time.Minute) + seconds*uint64(time.Second))
	absTime := time.Unix(0, 0).Add(duration)
	return absTime, nil
}

// converts the StopTime's CSV attributes and fills the departure/arrival time
func (st *StopTime) convertTimes() error {
	var err error
	st.DepartureTime, err = convertTime(st.CsvDepartureTime)
	if err != nil {
		return err
	}
	st.ArrivalTime, err = convertTime(st.CsvArrivalTime)
	if err != nil {
		return err
	}
	if st.DepartureTime.IsZero() && st.ArrivalTime.IsZero() {
		return fmt.Errorf("StopTime has no arrival time AND no departure time")
	}
	return nil
}

type Calendar struct {
	FeedId        string         `gorm:"primaryKey;index:pk_calendar" json:"feed_id"`
	ServiceId     string         `gorm:"primaryKey;index:pk_calendar" csv:"service_id" json:"service_id"`
	Monday        bool           `csv:"monday" json:"monday"`
	Tuesday       bool           `csv:"tuesday" json:"tuesday"`
	Wednesday     bool           `csv:"wednesday" json:"wednesday"`
	Thursday      bool           `csv:"thursday" json:"thursday"`
	Friday        bool           `csv:"friday" json:"friday"`
	Saturday      bool           `csv:"saturday" json:"saturday"`
	Sunday        bool           `csv:"sunday" json:"sunday"`
	CsvStartDate  string         `gorm:"-:all" csv:"start_date" json:"-"` //YYYYmmdd
	CsvEndDate    string         `gorm:"-:all" csv:"end_date" json:"-"`   //YYYYmmdd
	StartDate     time.Time      `json:"start_date"`
	EndDate       time.Time      `json:"end_date"`
	CalendarDates []CalendarDate `gorm:"foreignKey:FeedId,ServiceId;references:FeedId,ServiceId"`
}

func (Calendar) TableName() string {
	return "calendar"
}

func (c Calendar) GetWeekdayStatus(weekday time.Weekday) bool {
	switch weekday {
	case time.Monday:
		return c.Monday
	case time.Tuesday:
		return c.Tuesday
	case time.Wednesday:
		return c.Wednesday
	case time.Thursday:
		return c.Thursday
	case time.Friday:
		return c.Friday
	case time.Saturday:
		return c.Saturday
	case time.Sunday:
		return c.Sunday
	}
	return false
}

type ExceptionType uint

const (
	ExceptionTypeServiceAdded   ExceptionType = 1
	ExceptionTypeServiceRemoved ExceptionType = 2
)

type CalendarDate struct {
	FeedId        string        `gorm:"primaryKey;index:pk_calendardate" json:"feed_id"`
	ServiceId     string        `gorm:"primaryKey;index:pk_calendardate" csv:"service_id" json:"service_id"`
	Date          time.Time     `gorm:"primaryKey;index:pk_calendardate" json:"date"`
	CsvDate       string        `csv:"date" gorm:"-:all" json:"-"`           //YYYYmmdd
	ExceptionType ExceptionType `csv:"exception_type" json:"exception_type"` //1=added, 2=removed
}

type ServiceDay struct {
	Date      time.Time `gorm:"primaryKey;index:pk_servicedays"`
	FeedId    string    `gorm:"primaryKey;index:pk_servicedays"`
	ServiceId string    `gorm:"primaryKey;index:pk_servicedays"`
}

func (s ServiceDay) GetFeededService() FeededService {
	return FeededService{
		FeedId:    s.FeedId,
		ServiceId: s.ServiceId,
	}
}

type Feed struct {
	DisplayName   string `json:"display_name"` //added by us
	FeedId        string `gorm:"primaryKey;index:pk_feed" json:"feed_id"`
	PublisherName string `csv:"feed_publisher_name" json:"publisher_name"`
	PublisherUrl  string `csv:"feed_publisher_url" json:"publisher_url"`
	FeedLang      string `csv:"feed_lang" json:"feed_lang"`
	DefaultLang   string `csv:"default_lang" json:"default_lang"`
	Version       string `csv:"feed_version" json:"version"`
	ContactEmail  string `csv:"feed_contact_email" json:"contact_email"`
	ContactUrl    string `csv:"feed_contact_url" json:"contact_url"`
}
