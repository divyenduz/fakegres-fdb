package main

import (
	"flag"
	"log"
)

type config struct {
	columnar bool
	reset    bool
	pgPort   string
}

func getConfig() config {
	cfg := config{}
	flag.BoolVar(&cfg.columnar, "columnar", false, "Open the database in columnar mode")
	flag.BoolVar(&cfg.reset, "reset", false, "Reset the database on startup")
	flag.StringVar(&cfg.pgPort, "pg-port", "6000", "Port to listen on for PostgreSQL connections")
	flag.Parse()
	log.Println("cfg: ", cfg)
	return cfg
}
