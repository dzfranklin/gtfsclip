package main

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
)

func main() {
	if len(os.Args) != 3 {
		panic("Usage: gtfsclip <input_file> <output_file>")
	}
	inputName := os.Args[1]
	outputName := os.Args[2]

	minLat := 54.933020

	input, err := zip.OpenReader(inputName)
	if err != nil {
		panic(err)
	}

	// Step 1: Parse to find what to keep

	// Parse stops

	f, err := input.Open("stops.txt")
	if err != nil {
		panic(err)
	}
	r := csv.NewReader(f)
	h, err := r.Read()
	if err != nil {
		panic(err)
	}
	stopIDI := fieldIndex(h, "stop_id")
	stopLatI := fieldIndex(h, "stop_lat")

	stopInclusion := make(map[string]struct{})
	totalStops := 0
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}

		id := row[stopIDI]
		if id == "" {
			panic("empty id")
		}

		lat, err := strconv.ParseFloat(row[stopLatI], 64)
		if err != nil {
			panic(err)
		}
		if lat > minLat {
			stopInclusion[id] = struct{}{}
		}
		totalStops++
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("Parsed stops.txt")
	fmt.Printf("    %d stops in total\n", totalStops)
	fmt.Printf("    %d stops are within bounds\n", len(stopInclusion))

	// Parse trips

	f, err = input.Open("trips.txt")
	if err != nil {
		panic(err)
	}
	r = csv.NewReader(f)
	_, err = r.Read()
	if err != nil {
		panic(err)
	}

	totalTrips := 0
	for {
		_, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		totalTrips++
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("Parsed trips.txt")
	fmt.Printf("    %d trips in total\n", totalTrips)

	// Parse stop times

	f, err = input.Open("stop_times.txt")
	if err != nil {
		panic(err)
	}
	r = csv.NewReader(f)
	h, err = r.Read()
	if err != nil {
		panic(err)
	}
	tripIDI := fieldIndex(h, "trip_id")
	stopIDI = fieldIndex(h, "stop_id")

	tripStops := make(map[string][]string)
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}

		tripID := row[tripIDI]
		stopID := row[stopIDI]
		if tripID == "" || stopID == "" {
			panic("empty id")
		}

		tripStops[tripID] = append(tripStops[tripID], stopID)
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}

	tripInclusion := make(map[string]struct{})
	for tripID, stopIDs := range tripStops {
		isIncluded := false
		for _, stopID := range stopIDs {
			if _, ok := stopInclusion[stopID]; ok {
				isIncluded = true
				break
			}
		}

		if isIncluded {
			tripInclusion[tripID] = struct{}{}
		}
	}

	for trip := range tripInclusion {
		for _, stop := range tripStops[trip] {
			stopInclusion[stop] = struct{}{}
		}
	}

	if _, ok := tripInclusion["50-c688ed38d25efe9a36c2"]; ok {
		panic("Would keep Thameslink trip")
	}

	fmt.Println("Parsed stop_times.txt")
	fmt.Printf("    %d trips contain stops within bounds\n", len(tripInclusion))
	fmt.Printf("    Increased included stops to %d\n", len(stopInclusion))

	// Parse trips

	f, err = input.Open("trips.txt")
	if err != nil {
		panic(err)
	}
	r = csv.NewReader(f)
	h, err = r.Read()
	if err != nil {
		panic(err)
	}
	routeIDI := fieldIndex(h, "route_id")
	serviceIDI := fieldIndex(h, "service_id")
	tripIDI = fieldIndex(h, "trip_id")
	shapeIDI := slices.Index(h, "shape_id")

	tripsByRoute := make(map[string][]string)
	tripsByShape := make(map[string][]string)
	tripsByService := make(map[string][]string)
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}

		routeID := row[routeIDI]
		tripID := row[tripIDI]
		if routeID == "" {
			panic("empty id")
		}

		serviceID := row[serviceIDI]

		var shapeID string
		if shapeIDI >= 0 {
			shapeID = row[shapeIDI]
		}

		tripsByRoute[routeID] = append(tripsByRoute[routeID], tripID)
		if shapeID != "" {
			tripsByShape[shapeID] = append(tripsByShape[shapeID], tripID)
		}
		if serviceID != "" {
			tripsByService[serviceID] = append(tripsByService[shapeID], tripID)
		}
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}

	routeInclusion := make(map[string]struct{})
	for route, trips := range tripsByRoute {
		isIncluded := false
		for _, trip := range trips {
			if _, ok := tripInclusion[trip]; ok {
				isIncluded = true
				break
			}
		}
		if isIncluded {
			routeInclusion[route] = struct{}{}
		}
	}

	if _, ok := routeInclusion["50-2-172152"]; ok { // Thameslink
		panic("Would keep Thameslink route")
	}

	shapeInclusion := make(map[string]struct{})
	for shape, trips := range tripsByShape {
		isIncluded := false
		for _, trip := range trips {
			if _, ok := tripInclusion[trip]; ok {
				isIncluded = true
				break
			}
		}
		if isIncluded {
			shapeInclusion[shape] = struct{}{}
		}
	}

	serviceInclusion := make(map[string]struct{})
	for service, trips := range tripsByService {
		isIncluded := false
		for _, trip := range trips {
			if _, ok := tripInclusion[trip]; ok {
				isIncluded = true
				break
			}
		}
		if isIncluded {
			serviceInclusion[service] = struct{}{}
		}
	}

	if _, ok := serviceInclusion["50-2-401"]; ok {
		panic("Would keep Thameslink service")
	}

	fmt.Println("Parsed trips.txt")
	fmt.Printf("    %d routes contain a stop within bounds\n", len(routeInclusion))
	fmt.Printf("    %d shapes contain a stop within bounds\n", len(shapeInclusion))
	fmt.Printf("    %d services contain a stop within bounds\n", len(serviceInclusion))

	// Parse routes

	f, err = input.Open("routes.txt")
	if err != nil {
		panic(err)
	}
	r = csv.NewReader(f)
	h, err = r.Read()
	if err != nil {
		panic(err)
	}
	routeIDI = fieldIndex(h, "route_id")
	agencyIDI := fieldIndex(h, "agency_id")

	routesByAgency := make(map[string][]string)
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}

		routeID := row[routeIDI]
		agencyID := row[agencyIDI]

		if routeID == "" || agencyID == "" {
			panic("empty id")
		}

		routesByAgency[agencyID] = append(routesByAgency[agencyID], routeID)
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}

	agencyInclusion := make(map[string]struct{})
	for agency, routes := range routesByAgency {
		isIncluded := false
		for _, route := range routes {
			if _, ok := routeInclusion[route]; ok {
				isIncluded = true
				break
			}
		}
		if isIncluded {
			agencyInclusion[agency] = struct{}{}
		}
	}

	fmt.Println("Parsed routes.txt")
	fmt.Printf("    %d agencies contain a stop within bounds\n", len(agencyInclusion))

	// Step 2: Parse again and write out

	fmt.Println("Preparing to write output")
	fmt.Printf("    Including %d agencies\n", len(agencyInclusion))
	fmt.Printf("    Including %d routes\n", len(routeInclusion))
	fmt.Printf("    Including %d shapes\n", len(shapeInclusion))
	fmt.Printf("    Including %d stops\n", len(stopInclusion))

	outputZip, err := os.Create(outputName)
	if err != nil {
		panic(err)
	}
	outputW := zip.NewWriter(outputZip)

	// Copy agency.txt

	inF, err := input.Open("agency.txt")
	if err != nil {
		panic(err)
	}
	inR := csv.NewReader(inF)

	outF, err := outputW.Create("agency.txt")
	if err != nil {
		panic(err)
	}
	outW := csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI := fieldIndex(h, "agency_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count := 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := agencyInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote agency.txt (%d lines)\n", count+1)

	// Copy calendar.txt

	inF, err = input.Open("calendar.txt")
	if err != nil {
		panic(err)
	}
	inR = csv.NewReader(inF)

	outF, err = outputW.Create("calendar.txt")
	if err != nil {
		panic(err)
	}
	outW = csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI = fieldIndex(h, "service_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count = 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := serviceInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote calendar.txt (%d lines)\n", count+1)

	// Copy calendar_dates.txt

	inF, err = input.Open("calendar_dates.txt")
	if err != nil {
		panic(err)
	}
	inR = csv.NewReader(inF)

	outF, err = outputW.Create("calendar_dates.txt")
	if err != nil {
		panic(err)
	}
	outW = csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI = fieldIndex(h, "service_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count = 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := serviceInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote calendar_dates.txt (%d lines)\n", count+1)

	// Copy feed_info.txt

	inF, err = input.Open("feed_info.txt")
	if err == nil {
		outF, err = outputW.Create("feed_info.txt")
		if err != nil {
			panic(err)
		}

		_, err = io.Copy(outF, inF)
		if err != nil {
			panic(err)
		}

		fmt.Println("Wrote feed_info.txt")
	}

	// Copy routes.txt

	inF, err = input.Open("routes.txt")
	if err != nil {
		panic(err)
	}
	inR = csv.NewReader(inF)

	outF, err = outputW.Create("routes.txt")
	if err != nil {
		panic(err)
	}
	outW = csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI = fieldIndex(h, "route_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count = 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := routeInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote routes.txt (%d lines)\n", count+1)

	// Copy shapes.txt

	inF, err = input.Open("shapes.txt")
	if err == nil {
		inR = csv.NewReader(inF)

		outF, err = outputW.Create("shapes.txt")
		if err != nil {
			panic(err)
		}
		outW = csv.NewWriter(outF)

		h, err = inR.Read()
		if err != nil {
			panic(r)
		}
		idI = fieldIndex(h, "shape_id")
		err = outW.Write(h)
		if err != nil {
			panic(err)
		}

		count = 0
		for {
			row, err := inR.Read()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				panic(err)
			}
			id := row[idI]

			if _, ok := shapeInclusion[id]; ok {
				err := outW.Write(row)
				if err != nil {
					panic(err)
				}
				count++
			}
		}

		outW.Flush()
		err = outW.Error()
		if err != nil {
			panic(err)
		}

		fmt.Printf("Wrote shapes.txt (%d lines)\n", count+1)
	}

	// Copy stop_times.txt

	inF, err = input.Open("stop_times.txt")
	if err != nil {
		panic(err)
	}
	inR = csv.NewReader(inF)

	outF, err = outputW.Create("stop_times.txt")
	if err != nil {
		panic(err)
	}
	outW = csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI = fieldIndex(h, "trip_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count = 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := tripInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote stop_times.txt (%d lines)\n", count+1)

	// Copy stops.txt

	inF, err = input.Open("stops.txt")
	if err != nil {
		panic(err)
	}
	inR = csv.NewReader(inF)

	outF, err = outputW.Create("stops.txt")
	if err != nil {
		panic(err)
	}
	outW = csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI = fieldIndex(h, "stop_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count = 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := stopInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote stops.txt (%d lines)\n", count+1)

	// Copy trips.txt

	inF, err = input.Open("trips.txt")
	if err != nil {
		panic(err)
	}
	inR = csv.NewReader(inF)

	outF, err = outputW.Create("trips.txt")
	if err != nil {
		panic(err)
	}
	outW = csv.NewWriter(outF)

	h, err = inR.Read()
	if err != nil {
		panic(r)
	}
	idI = fieldIndex(h, "trip_id")
	err = outW.Write(h)
	if err != nil {
		panic(err)
	}

	count = 0
	for {
		row, err := inR.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}
		id := row[idI]

		if _, ok := tripInclusion[id]; ok {
			err := outW.Write(row)
			if err != nil {
				panic(err)
			}
			count++
		}
	}

	outW.Flush()
	err = outW.Error()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote stops.txt (%d lines)\n", count+1)

	// Finalize

	err = input.Close()
	if err != nil {
		panic(err)
	}

	err = outputW.Close()
	if err != nil {
		panic(err)
	}

	err = outputZip.Close()
	if err != nil {
		panic(err)
	}
}

func fieldIndex(header []string, field string) int {
	idx := slices.Index(header, field)
	if idx == -1 {
		panic(fmt.Sprintf("field %s not found", field))
	}
	return idx
}
