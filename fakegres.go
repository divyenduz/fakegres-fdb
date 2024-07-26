//go:build !repl
// +build !repl

package main

import (
	"log"
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type config struct {
	id     string
	pgPort string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}
		if arg == "--pg-port" {
			cfg.pgPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.pgPort == "" {
		log.Fatal("Missing required parameter: --pg-port")
	}

	return cfg
}

func main() {
	cfg := getConfig()

	fdb.MustAPIVersion(710)
	db := fdb.MustOpenDefault()

	// Note: uncomment to clear all keys in the database
	// db.Transact(func(tr fdb.Transaction) (interface{}, error) {
	// 	tr.ClearRange(fdb.KeyRange{Begin: fdb.Key{}, End: fdb.Key{0xFF}})
	// 	log.Println("All keys have been deleted from the database.")
	// 	return nil, nil
	// })

	runPgServer(cfg.pgPort, db)
}
