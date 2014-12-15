package bloomdb

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"log"
	"strings"
	"text/template"
	"time"
)

var fns = template.FuncMap{
	"eq": func(x, y interface{}) bool {
		return x == y
	},
	"sub": func(y, x int) int {
		return x - y
	},
}

type upsertInfo struct {
	Table        string
	TempTable    string
	IdColumn     string
	HasRevisions bool
	Columns      []string
}

func buildQuery(table string, tempTable string, idColumn string, columns []string, hasRevisions bool) (string, string, error) {
	upsertBuf := &bytes.Buffer{}
	t, err := template.New("upsert.sql.template").Funcs(fns).ParseFiles("sql/upsert.sql.template")
	if err != nil {
		return "", "", err
	}
	info := upsertInfo{table, tempTable, idColumn, hasRevisions, columns}
	err = t.Execute(upsertBuf, info)
	if err != nil {
		return "", "", err
	}

	revisionBuf := &bytes.Buffer{}
	if hasRevisions {
		t, err = template.New("updaterevisions.sql.template").Funcs(fns).ParseFiles("sql/updaterevisions.sql.template")
		if err != nil {
			return "", "", err
		}
		err = t.Execute(revisionBuf, info)
		if err != nil {
			return "", "", err
		}
	}

	return upsertBuf.String(), revisionBuf.String(), nil
}

func Upsert(db *sql.DB, table string, idColumn string, columns []string, rows chan []string, hasRevisions bool) error {
	// Can't create a temporary table inside a non-temporary schema, so just
	// replace the periods, if present, with semicolons to avoid errors.
	tempTable := strings.Replace(table, ".", "_", -1) + "_temp"

	query, revisionQuery, err := buildQuery(table, tempTable, idColumn, columns, hasRevisions)
	if err != nil {
		return err
	}

	startTime := time.Now()
	log.Println("Starting database write...")

	txn, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = txn.Exec("CREATE TEMP TABLE " + tempTable + "(LIKE " + table + ")")
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(tempTable, columns...))
	if err != nil {
		return err
	}

	rowsProcessed := 0
	for rawRow := range rows {
		row := make([]interface{}, len(rawRow))
		for i, column := range rawRow {
			if column == "" {
				row[i] = nil
			} else {
				row[i] = column
			}
		}

		_, err = stmt.Exec(row...)
		if err != nil {
			fmt.Println("table", table, "row", row)
			return err
		}

		rowsProcessed++

		if rowsProcessed%100000 == 0 {
			log.Printf("Processed %d rows...", rowsProcessed)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	// Use this transaction just for the copy, and start another one afterward
	// for better performance.
	err = txn.Commit()
	if err != nil {
		return err
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	duration = duration / time.Second
	log.Printf("Processed %d rows total, took %d:%02d\n", rowsProcessed,
		duration/60, duration%60)

	log.Println("Creating table index")
	_, err = db.Exec("CREATE UNIQUE INDEX ON " + tempTable + "(" + idColumn + ")")
	if err != nil {
		return err
	}

	log.Println("Analyzing table")
	_, err = db.Exec("ANALYZE " + tempTable)

	txn, err = db.Begin()
	if err != nil {
		return err
	}

	if revisionQuery != "" {
		log.Println("Calculating revisions...")
		res, err := txn.Exec(revisionQuery)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		log.Printf("Updated revision on %d rows", affected)
	}

	log.Println("Performing upsert...")
	_, err = txn.Exec(query)
	if err != nil {
		return err
	}

	log.Println("Committing transaction...")
	err = txn.Commit()
	if err != nil {
		return err
	}
	log.Println("Done")
	return nil
}
