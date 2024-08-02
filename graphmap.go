package trainmapdb

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/dominikbraun/graph"
)

type feededStopId struct {
	feedId string
	stopId string
	name   string
}

func (s Stop) getFeededStopId() feededStopId {
	return feededStopId{
		feedId: s.FeedId,
		stopId: s.StopId,
		name:   s.StopName,
	}
}

// represents travel between 2 stops taking up a given amount of time
type segment struct {
	source      Stop
	target      Stop
	travelTime  time.Duration
	hasDistance bool
	distance    float64
}

func (s *segment) getDistance() float64 {
	if s.hasDistance {
		return s.distance
	}
	s.distance = s.source.GetPoint().getDistTo(s.target.GetPoint())
	s.hasDistance = true
	return s.distance
}

// a map of a transport network (here rail), used to interpolate the trajectory or large hops based on smaller hops
type NetworkMap struct {
	ParentStops      map[feededStopId]Stop         //all parent stops
	CityAssociations map[feededStopId]feededStopId //maps a stop to its parent stop; parent stop is self if no stops nearby, otherwise the nearest first added one
	Graph            graph.Graph[feededStopId, Stop]
}

// returns a new empty network map
func NewNetworkMap() *NetworkMap {
	stopHashFunc := func(stop Stop) feededStopId {
		return stop.getFeededStopId()
	}
	return &NetworkMap{
		CityAssociations: make(map[feededStopId]feededStopId),
		ParentStops:      make(map[feededStopId]Stop),
		Graph:            graph.New[feededStopId, Stop](stopHashFunc, graph.Weighted()),
	}
}

type tripMap struct {
	segmentMap map[feededStopId]map[feededStopId]time.Duration
	stopMap    map[feededStopId]Stop
}

func newTripMap() tripMap {
	segmentMap := make(map[feededStopId]map[feededStopId]time.Duration)
	stopMap := make(map[feededStopId]Stop)
	return tripMap{
		segmentMap: segmentMap,
		stopMap:    stopMap,
	}
}

func (tm tripMap) getAllSegments() []segment {
	var segments []segment
	for sourceFsi, targetFsiToTravelTime := range tm.segmentMap {
		for targetFsi, travelTime := range targetFsiToTravelTime {
			segments = append(segments, segment{
				source:     tm.stopMap[sourceFsi],
				target:     tm.stopMap[targetFsi],
				travelTime: travelTime,
			})
		}
	}
	return segments
}

// add the segment to the map if not indexed yet, updates the time if it does exist
func (tm *tripMap) updateSegment(seg segment) {
	//make sure stops exist
	tm.stopMap[seg.source.getFeededStopId()] = seg.source
	tm.stopMap[seg.target.getFeededStopId()] = seg.target
	//do the actual update
	theirTravelTime, exists := tm.containsUndirectional(seg)
	shouldAdd := true
	_, existsInThisDirection := tm.containsDirectional(seg)
	if exists {
		shouldAdd = theirTravelTime < seg.travelTime
	}
	if !shouldAdd {
		return
	}
	if exists && !existsInThisDirection {
		//no need to check for nil, it already exists
		tm.segmentMap[seg.target.getFeededStopId()][seg.source.getFeededStopId()] = seg.travelTime
		return
	}
	if _, ok := tm.segmentMap[seg.source.getFeededStopId()]; !ok {
		tm.segmentMap[seg.source.getFeededStopId()] = make(map[feededStopId]time.Duration)
	}
	tm.segmentMap[seg.source.getFeededStopId()][seg.target.getFeededStopId()] = seg.travelTime
}

func (tm tripMap) containsUndirectional(seg segment) (time.Duration, bool) {
	travelTime, ok := tm.containsDirectional(seg)
	if ok {
		return travelTime, true
	}
	reversedSegment := segment{
		source:     seg.target,
		target:     seg.source,
		travelTime: seg.travelTime,
	}
	return tm.containsDirectional(reversedSegment)
}

func (tm tripMap) containsDirectional(seg segment) (time.Duration, bool) {
	targetToTravelTime, ok := tm.segmentMap[seg.source.getFeededStopId()]
	if !ok {
		return time.Duration(0), false
	}
	travelTime, ok := targetToTravelTime[seg.target.getFeededStopId()]
	if !ok {
		return time.Duration(0), false
	}
	return travelTime, true
}

// builds a network map using the given trips as a reference. it's assumed the stops and stop times are given.
func NewNetworkMapFromTrips(trips []Trip) (*NetworkMap, error) {
	log.Default().Println("Building full segment list...")
	tm := newTripMap() //avoid duplicated
	for _, trip := range trips {
		for _, tripSegment := range trip.getSegments() {
			tm.updateSegment(tripSegment)
		}
	}
	allSegments := tm.getAllSegments()

	log.Default().Println("Sorting segment list...")
	sort.Slice(allSegments, func(i, j int) bool {
		//remove travel time sorting, use distance instead
		// return allSegments[i].travelTime < allSegments[j].travelTime
		return allSegments[i].getDistance() < allSegments[j].getDistance()
	})
	log.Default().Println("Building network map...")
	nm := NewNetworkMap()
	for _, seg := range allSegments {
		err := nm.processSegment(seg)
		if err != nil {
			return nil, err
		}
	}

	log.Default().Println("Done building network map!")

	return nm, nil
}

// builds a network map using all trips, but filters by those having a given route type
func NewNetworkMapFromRouteTypes(trips []Trip, routeType RouteType) (*NetworkMap, error) {
	var filteredTrips []Trip
	for _, trip := range trips {
		if trip.Route.RouteType == routeType {
			filteredTrips = append(filteredTrips, trip)
		}
	}
	return NewNetworkMapFromTrips(filteredTrips)
}

func NewNetworkMapFromLocation(f Fetcher, point Point) (*NetworkMap, error) {
	trips, err := f.GetTripsInsidePointInterval(point, point)
	if err != nil {
		return nil, err
	}
	return NewNetworkMapFromTrips(trips)
}

func NewNetworkMapFromPointBox(f Fetcher, pt1, pt2 Point) (*NetworkMap, error) {
	trips, err := f.GetTripsInsidePointInterval(pt1, pt2)
	if err != nil {
		return nil, err
	}
	return NewNetworkMapFromTrips(trips)
}

// adds a stop to the network map if it doesn't already exist
func (nm *NetworkMap) EnsureCloseStopInGraph(stop Stop) (feededStopId, error) {
	//if contained, return assigned parent stop
	if nm.Contains(stop) {
		return nm.CityAssociations[nm.CityAssociations[stop.getFeededStopId()]], nil
	}
	const DIST_THRESHOLD = 0.8 //in km
	//if not contained, check for close stops and add association
	for _, parentStop := range nm.ParentStops {
		if stop.GetPoint().getDistTo(parentStop.GetPoint()) < DIST_THRESHOLD {
			nm.CityAssociations[stop.getFeededStopId()] = parentStop.getFeededStopId()
			return parentStop.getFeededStopId(), nil
		}
	}
	//if no close stops, add new to graph AND parent associations
	err := nm.Graph.AddVertex(stop, graph.VertexAttribute("label", stop.StopName))
	if err != nil {
		return feededStopId{}, err
	}
	nm.ParentStops[stop.getFeededStopId()] = stop
	nm.CityAssociations[stop.getFeededStopId()] = stop.getFeededStopId()
	return stop.getFeededStopId(), nil
}

// returns whether or not the stop is stored in the network map
func (nm *NetworkMap) Contains(stop Stop) bool {
	_, ok := nm.CityAssociations[stop.getFeededStopId()]
	return ok
}

// sets the segment on the graph with the given composition, whether aleady there or not (nil = elementary segment)
func (nm *NetworkMap) setSegmentOnGraph(seg segment, composition []segment) error {
	colorString := "black"
	if composition != nil && len(composition) != 1 {
		colorString = "red"
	}
	edgeLabel := fmt.Sprintf("%v (%fkm)", seg.travelTime, seg.getDistance())
	weight := int(seg.travelTime)
	err := nm.Graph.AddEdge(
		seg.source.getFeededStopId(),
		seg.target.getFeededStopId(),
		graph.EdgeWeight(weight),
		graph.EdgeAttribute("color", colorString),
		graph.EdgeAttribute("label", edgeLabel),
		graph.EdgeData(composition),
	)
	if err == graph.ErrEdgeAlreadyExists {
		edge, err := nm.Graph.Edge(seg.source.getFeededStopId(), seg.target.getFeededStopId())
		if err != nil {
			return err
		}
		var minWeight int
		if weight < edge.Properties.Weight {
			minWeight = weight
		} else {
			minWeight = edge.Properties.Weight
		}

		return nm.Graph.UpdateEdge(
			seg.source.getFeededStopId(),
			seg.target.getFeededStopId(),
			graph.EdgeWeight(minWeight),
			graph.EdgeAttribute("color", colorString),
			graph.EdgeAttribute("label", edgeLabel),
			graph.EdgeData(composition),
		)
	}
	if err != nil {
		return err
	}
	return nil
}

// tries adding the given segment to add extra info on the map.
// WARN: the segments should be given in ascending order to guarantee the graph is correctly built!
func (nm *NetworkMap) processSegment(seg segment) error {
	//add stops if not already in there
	segSourceFsid, err := nm.EnsureCloseStopInGraph(seg.source)
	if err != nil {
		return err
	}
	segTargetFsid, err := nm.EnsureCloseStopInGraph(seg.target)
	if err != nil {
		return err
	}
	seg = segment{
		source:     nm.ParentStops[segSourceFsid],
		target:     nm.ParentStops[segTargetFsid],
		travelTime: seg.travelTime,
	}

	//try getting a path
	// println("Getting path...")
	path, err := graph.ShortestPath(nm.Graph, segSourceFsid, segTargetFsid)
	// if no path, add a new one
	if err == graph.ErrTargetNotReachable {
		// println("-> no path, adding new...")
		return nm.setSegmentOnGraph(seg, nil)
	}

	//rethrow other errors
	if err != nil {
		return err
	}
	// println("-> yes path, checking durations...")

	//handle case where segment already exists, re add with current composition, travel time check is done by setsegmentongraph
	if len(path) == 2 {
		edge, err := nm.Graph.Edge(segSourceFsid, segTargetFsid)
		if err != nil {
			return err
		}
		return nm.setSegmentOnGraph(seg, edge.Properties.Data.([]segment))
	}

	//check path total time
	pathWeight := 0
	var segments []segment
	//sum the time of all edges in the graph
	//also convert []fsid to []segment to allow for composite paths if need be
	for i := range path {
		if i == 0 {
			continue
		}
		sourceFsid, targetFsid := path[i-1], path[i]
		pathSourceStop, err := nm.Graph.Vertex(sourceFsid)
		if err != nil {
			return err
		}
		pathTargetStop, err := nm.Graph.Vertex(targetFsid)
		if err != nil {
			return err
		}
		edge, err := nm.Graph.Edge(path[i-1], path[i])
		if err != nil {
			return err
		}
		pathWeight += edge.Properties.Weight
		segments = append(segments, segment{
			source:     pathSourceStop,
			target:     pathTargetStop,
			travelTime: time.Duration(edge.Properties.Weight),
		})
	}

	ourWeight := int(seg.travelTime)

	const PATH_THRESHOLD_FACTOR = 1.66
	// if our path is way longer than this one, then just make a new edge
	if float64(ourWeight)*PATH_THRESHOLD_FACTOR < float64(pathWeight) {
		// println("--> path is too long, adding a new one")
		return nm.setSegmentOnGraph(seg, nil)
	}
	// println("--> path short enough, adding composite segment")

	return nm.setSegmentOnGraph(seg, segments)
}

// returns the path the trip takes along its way
func (nm *NetworkMap) decomposeTripSegment(seg segment) ([]segment, error) {
	sourceFsid := nm.CityAssociations[seg.source.getFeededStopId()]
	targetFsid := nm.CityAssociations[seg.target.getFeededStopId()]
	edge, err := nm.Graph.Edge(sourceFsid, targetFsid)
	// should always exist, handle ErrEdgeNotFound like other errs
	if err != nil {
		return nil, err
	}
	newSeg := segment{
		source:     nm.ParentStops[sourceFsid],
		target:     nm.ParentStops[targetFsid],
		travelTime: time.Duration(edge.Properties.Weight),
	}
	return nm.decomposeGraphSegment(newSeg)
}

// decomposes a segment which is guaranteed to be on the graph
func (nm *NetworkMap) decomposeGraphSegment(startSegment segment) ([]segment, error) {
	var processed, processStack []segment
	processStack = append(processStack, startSegment)
	for len(processStack) > 0 {
		var currentSeg segment
		currentSeg, processStack = processStack[0], processStack[1:]
		edge, err := nm.Graph.Edge(currentSeg.source.getFeededStopId(), currentSeg.target.getFeededStopId())
		if err != nil {
			return nil, err
		}
		composition := edge.Properties.Data.([]segment)
		if composition != nil {
			processStack = append(composition, processStack...) //Add to head, not tail
		} else {
			processed = append(processed, currentSeg)
		}
	}
	return processed, nil
}

// returns the stoptimes of the trip as a segment slice
func (trip Trip) getSegments() []segment {
	var segments []segment
	stopTimes := trip.StopTimes
	sort.Slice(stopTimes, func(i, j int) bool {
		return stopTimes[i].DepartureTime.Before(stopTimes[j].DepartureTime)
	})
	for i := range stopTimes {
		if i == 0 {
			continue
		}
		sourceStopTime := stopTimes[i-1]
		targetStopTime := stopTimes[i]
		travelTime := targetStopTime.ArrivalTime.Sub(sourceStopTime.DepartureTime)
		if travelTime < 0 { //sanity check
			panic(fmt.Errorf("got negative travel time: %v between %v and %v", travelTime, sourceStopTime.DepartureTime, targetStopTime.ArrivalTime))
		}
		seg := segment{
			source:     *sourceStopTime.Stop,
			target:     *targetStopTime.Stop,
			travelTime: travelTime,
		}
		segments = append(segments, seg)
	}
	return segments
}
