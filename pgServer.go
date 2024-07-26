package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

type pgServer struct {
	conn net.Conn
	db   fdb.Transactor
}

func (pgs pgServer) done(buf []byte, msg string) {
	buf = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err := pgs.conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write query response: %s", err)
	}
}

func (pgs pgServer) writePgResult(res *pgResult) {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.fieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.fieldTypes[i]],
		})
	}
	buf := rd.Encode(nil)
	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("Failed to marshal cell: %s\n", err)
				return
			}

			dr.Values = append(dr.Values, bs)
		}

		buf = dr.Encode(buf)
	}

	pgs.done(buf, fmt.Sprintf("SELECT %d", len(res.rows)))
}

func (pgs pgServer) handleStartupMessage(pgconn *pgproto3.Backend) error {
	startupMessage, err := pgconn.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("Error receiving startup message: %s", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = pgs.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("Error sending ready for query: %s", err)
		}

		return nil
	case *pgproto3.SSLRequest:
		_, err = pgs.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("Error sending deny SSL request: %s", err)
		}

		return pgs.handleStartupMessage(pgconn)
	default:
		return fmt.Errorf("Unknown startup message: %#v", startupMessage)
	}
}

func (pgs pgServer) handleMessage(pgc *pgproto3.Backend) error {
	msg, err := pgc.Receive()
	if err != nil {
		return fmt.Errorf("Error receiving message: %s", err)
	}

	switch t := msg.(type) {
	case *pgproto3.Query:
		stmts, err := pgquery.Parse(t.String)
		if err != nil {
			return fmt.Errorf("Error parsing query: %s", err)
		}

		if len(stmts.GetStmts()) > 1 {
			return fmt.Errorf("Only make one request at a time.")
		}

		stmt := stmts.GetStmts()[0]

		// Handle SELECTs here
		s := stmt.GetStmt().GetSelectStmt()
		if s != nil {
			pe := newPgEngine(pgs.db)
			res, err := pe.executeSelect(s)
			if err != nil {
				return err
			}

			pgs.writePgResult(res)
			return nil
		} else {
			pe := newPgEngine(pgs.db)
			pe.execute(*stmts)
		}

		pgs.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("Received message other than Query from client: %s", msg)
	}

	return nil
}

func (pgs pgServer) handle() {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pgs.conn), pgs.conn)
	defer pgs.conn.Close()

	err := pgs.handleStartupMessage(pgc)
	if err != nil {
		log.Println(err)
		return
	}

	for {
		err := pgs.handleMessage(pgc)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func runPgServer(port string, db fdb.Transactor) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := pgServer{conn, db}
		go pc.handle()
	}
}
