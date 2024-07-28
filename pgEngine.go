package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type pgEngine struct {
	db fdb.Transactor
}

func newPgEngine(db fdb.Transactor) pgEngine {
	return pgEngine{db}
}

func (pe pgEngine) execute(tree pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.executeCreate(c)
		}

		if c := n.GetInsertStmt(); c != nil {
			return pe.executeInsert(c)
		}

		if c := n.GetDeleteStmt(); c != nil {
			return pe.executeDelete(c)
		}

		if c := n.GetSelectStmt(); c != nil {
			_, err := pe.executeSelect(c)
			return err
		}
	}

	return nil
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

/*
Parse the create table SQL statement and create an equivalent KV structure in the database.

Example:

# The following SQL

```sql
create table user (age int, name text);
```

# Will produce the following KV structure

```
catalog/table/user: "" (empty value to mark that the table exists)
catalog/table/user/age: int
catalog/table/user/name: text
```

Keys in FoundationDB are globally sorted, so retrieving all the metadata for a table is
usually a single query.
*/
func (pe pgEngine) executeCreate(stmt *pgquery.CreateStmt) error {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{tbl.Name})

	_, err = pe.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

		if tr.Get(tableKey).MustGet() != nil {
			log.Printf("Table %s already exists", tbl.Name)
			return
		}

		// Note: table exists, marked by empty value and table name as key
		tr.Set(tableSS.Pack(tuple.Tuple{tbl.Name}), []byte(""))

		for _, c := range stmt.TableElts {
			cd := c.GetColumnDef()

			// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
			var columnType string
			for _, n := range cd.TypeName.Names {
				if columnType != "" {
					columnType += "."
				}
				columnType += n.GetString_().Str
			}
			tr.Set(tableSS.Pack(tuple.Tuple{tbl.Name, cd.Colname}), []byte(columnType))
		}

		return
	})

	if err != nil {
		return fmt.Errorf("could not create table: %s", err)
	}

	return nil
}

/*

Get the table definition from the database. This can be done with a single range query.

*/

func (pe pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	// TODO: check if table exists, etc.
	tbl.Name = name

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	tableSS := catalogDir.Sub("table")

	_, err = pe.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		ri := rtr.GetRange(tableSS.Sub(name), fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()
		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableSS.Unpack(kv.Key)

			// Note: deconstruct the key from catalog/table/user/age and extract the column name
			tbl.ColumnNames = append(tbl.ColumnNames, t[1].(string))
			tbl.ColumnTypes = append(tbl.ColumnTypes, string(kv.Value))
		}
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not get table defn: %s", err)
	}
	return &tbl, err
}

/*

Parse the insert statement and insert data into the table.

Example:

The following SQL

```sql
insert into user values(14, 'garry'), (20, 'ted');
```

Note that since keys are sorted, CREATE TABLE and positional ORDER of INSERT can be different.

Will produce the following KV structure

```
data/table_data/user/age/72746a7f-727f-4e0a-88f1-d983fea5c158: 14
data/table_data/user/age/34e7ff77-1bed-4ebd-be56-4b966e67c595: 20
data/table_data/user/name/72746a7f-727f-4e0a-88f1-d983fea5c158: garry
data/table_data/user/name/34e7ff77-1bed-4ebd-be56-4b966e67c595: ted
```

Keys in FoundationDB are globally sorted, so the data for this table would be a single query.
However, in this structure (column first) we will receive the table cells in age, age, name, name order and we will need to
collect them in order in select.

This property of Foundation DB is very interesting. Quoting the docs: https://apple.github.io/foundationdb/data-modeling.html
> You can make your model row-oriented or column-oriented by placing either the row or column first
> in the tuple, respectively. Because the lexicographic order sorts tuple elements from left to right,
> access is optimized for the element placed first. Placing the row first makes it efficient to read all
> the cells in a particular row; reversing the order makes reading a column more efficient.

We can insert columnar and row based data in the same table in same transaction and still be able to read them efficiently.
Note for future.

If this was row based, the keys in the database would be:

data/table_data/user/age/72746a7f-727f-4e0a-88f1-d983fea5c158: 14
data/table_data/user/name/72746a7f-727f-4e0a-88f1-d983fea5c158: garry
data/table_data/user/age/34e7ff77-1bed-4ebd-be56-4b966e67c595: 20
data/table_data/user/name/34e7ff77-1bed-4ebd-be56-4b966e67c595: ted

And reading them in select would be easier.
*/

func (pe pgEngine) executeInsert(stmt *pgquery.InsertStmt) error {
	tblName := stmt.Relation.Relname
	slct := stmt.GetSelectStmt().GetSelectStmt()

	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return err
	}

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{tblName})

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	_, err = pe.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		if tr.Get(tableKey).MustGet() == nil {
			log.Printf("Table %s does not exist", tblName)
			return
		}

		for _, values := range slct.ValuesLists {
			id := uuid.New().String()
			columnIndex := 0
			maxColumnIndex := len(tbl.ColumnNames) - 1
			for _, value := range values.GetList().Items {
				if c := value.GetAConst(); c != nil {
					if s := c.Val.GetString_(); s != nil {
						// Columnar data
						tr.Set(tableDataSS.Pack(tuple.Tuple{tblName, "c", tbl.ColumnNames[columnIndex], id}), []byte(s.Str))
						log.Printf("Inserted key c: %s", tableDataSS.Pack(tuple.Tuple{tblName, "c", tbl.ColumnNames[columnIndex], id}))
						// Row based data
						tr.Set(tableDataSS.Pack(tuple.Tuple{tblName, "r", id, tbl.ColumnNames[columnIndex]}), []byte(s.Str))
						log.Printf("Inserted key r: %s", tableDataSS.Pack(tuple.Tuple{tblName, "r", id, tbl.ColumnNames[columnIndex]}))

						if columnIndex < maxColumnIndex {
							columnIndex += 1
						}
						continue
					}

					if i := c.Val.GetInteger(); i != nil {
						// TODO: better convert in to byte[], with this conversion, it ends up being a string
						valueJson, _ := json.Marshal(i.Ival)
						// Columnar data
						tr.Set(tableDataSS.Pack(tuple.Tuple{tblName, "c", tbl.ColumnNames[columnIndex], id}), valueJson)
						log.Printf("Inserted key c: %s", tableDataSS.Pack(tuple.Tuple{tblName, "c", tbl.ColumnNames[columnIndex], id}))
						// Row based data
						tr.Set(tableDataSS.Pack(tuple.Tuple{tblName, "r", id, tbl.ColumnNames[columnIndex]}), valueJson)
						log.Printf("Inserted key r: %s", tableDataSS.Pack(tuple.Tuple{tblName, "r", id, tbl.ColumnNames[columnIndex]}))

						if columnIndex < maxColumnIndex {
							columnIndex += 1
						}
						continue
					}
				}

				return nil, fmt.Errorf("unknown value type: %s", value)
			}
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("could not insert into the table table: %s", err)
	}

	return nil
}

/*

Parse the delete statement and delete data from the table.
Currently, this doesn't support where clause and deletes all the data from the table.

*/

func (pe pgEngine) executeDelete(stmt *pgquery.DeleteStmt) error {

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{stmt.Relation.Relname})

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	// TODO: implement where, delete for now deletes everything from the table

	_, err = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if tr.Get(tableKey).MustGet() == nil {
			log.Printf("Table %s does not exist", stmt.Relation.Relname)
			return nil, nil
		}

		ri := tr.GetRange(tableDataSS, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()
		for ri.Advance() {
			kv := ri.MustGet()
			tr.Clear(kv.Key)
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("could not delete table: %s", err)
	}
	return nil
}

type pgResult struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
}

/*

Parse the select statement and return the result.

Example:

The following SQL:

```sql
select name, age from customer;
```

Will produce the following KV structure:

```
data/table_data/user/age/72746a7f-727f-4e0a-88f1-d983fea5c158: 14
data/table_data/user/age/34e7ff77-1bed-4ebd-be56-4b966e67c595: 20
data/table_data/user/name/72746a7f-727f-4e0a-88f1-d983fea5c158: garry
data/table_data/user/name/34e7ff77-1bed-4ebd-be56-4b966e67c595: ted
```

The Select code collects them into [[14, garry], [20, ted]] and returns the result accordingly.
*/

func (pe pgEngine) executeSelectColumnar(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}

		results.fieldTypes = append(results.fieldTypes, fieldType)
	}

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	_, _ = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		query := tableDataSS.Pack(tuple.Tuple{tbl.Name, "c"})
		rangeQuery, _ := fdb.PrefixRange(query)
		ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		var columnOrder []string
		var targetRows [][]any
		targetRows = append(targetRows, []any{})
		rowIndex := -1
		lastColumn := ""
		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableDataSS.Unpack(kv.Key)

			currentTableName := t[0].(string)
			currentColumnFormat := t[1].(string)
			currentColumnName := t[2].(string)
			currentInternalRowId := t[3].(string)
			log.Println("fetching row metadata: ", currentTableName, currentColumnFormat, currentColumnName, currentInternalRowId)
			if currentColumnName != lastColumn {
				rowIndex = 0
				lastColumn = currentColumnName
				columnOrder = append(columnOrder, currentColumnName)
			} else {
				targetRows = append(targetRows, []any{})
			}

			for _, target := range results.fieldNames {
				if target == currentColumnName {
					targetRows[rowIndex] = append(targetRows[rowIndex], string(kv.Value))
				}
			}
			rowIndex += 1
		}
		results.fieldNames = columnOrder

		// TODO: don't add empty arrays in the first place
		var targetRowsFinal [][]any
		targetRows = append(targetRows, []any{})
		for _, row := range targetRows {
			if len(row) > 0 {
				targetRowsFinal = append(targetRowsFinal, row)
			}
		}
		results.rows = targetRowsFinal
		return results, nil
	})

	return results, nil
}

func (pe pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}

		results.fieldTypes = append(results.fieldTypes, fieldType)
	}

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	_, _ = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		query := tableDataSS.Pack(tuple.Tuple{tbl.Name, "r"})
		rangeQuery, _ := fdb.PrefixRange(query)
		ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		var targetRows [][]any
		targetRows = append(targetRows, []any{})
		rowIndex := 0
		columnOrder := []string{}
		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableDataSS.Unpack(kv.Key)

			currentTableName := t[0].(string)
			currentColumnFormat := t[1].(string)
			currentInternalRowId := t[2].(string)
			currentColumnName := t[3].(string)
			log.Println("fetching row metadata: ", currentTableName, currentColumnFormat, currentColumnName, currentInternalRowId)

			if len(columnOrder) < len(results.fieldNames) {
				columnOrder = append(columnOrder, currentColumnName)
			}
			if len(targetRows[rowIndex]) == len(results.fieldNames) {
				rowIndex += 1
				targetRows = append(targetRows, []any{})
			}

			targetRows[rowIndex] = append(targetRows[rowIndex], string(kv.Value))
		}
		results.fieldNames = columnOrder

		// TODO: don't add empty arrays in the first place
		var targetRowsFinal [][]any
		targetRows = append(targetRows, []any{})
		for _, row := range targetRows {
			if len(row) > 0 {
				targetRowsFinal = append(targetRowsFinal, row)
			}
		}
		results.rows = targetRowsFinal
		return results, nil
	})

	return results, nil
}
