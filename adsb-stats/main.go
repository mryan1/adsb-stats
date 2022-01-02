package main

import (
	"fmt"
	"log"
	"net/http"
)

type aircraft struct {
	ICAO             string `json:"ICAO"`
	Registration     string `json:"registration"`
	ManufacturerName string `json:"manufacture"`
	Model            string `json:"model"`
	SerialNumber     string `json:"serialnumber"`
	Owner            string `json:"owner"`
	YearBuilt        string `json:"yearbuilt"`
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("<h1>adsb stats</h1>"))
}

func aircraftHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "The id query parameter is missing", http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "<h1>The user id is: %s</h1>", id)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", indexHandler)
	mux.HandleFunc("/user", userHandler)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
