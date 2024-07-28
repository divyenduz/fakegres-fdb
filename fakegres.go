package main

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func main() {
	cfg := getConfig()

	fdb.MustAPIVersion(710)
	db := fdb.MustOpenDefault()

	if cfg.reset {
		db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.ClearRange(fdb.KeyRange{Begin: fdb.Key{}, End: fdb.Key{0xFF}})
			log.Println("All keys have been deleted from the database.")
			return nil, nil
		})
	}

	runPgServer(cfg.pgPort, db, cfg)
}
