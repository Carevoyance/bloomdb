package bloomdb

import (
	"bytes"
	"database/sql"
	"github.com/lib/pq"
	"text/template"
	"fmt"
        "strings"
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
	Table     string
        TempTable string
        IdColumn  string
	Columns   []string
}

func buildQuery(table string, tempTable string, idColumn string, columns []string) (string, error) {
	buf := new(bytes.Buffer)
	t, err := template.New("upsert.sql.template").Funcs(fns).ParseFiles("sql/upsert.sql.template")
	if err != nil {
		return "", err
	}
	info := upsertInfo{table, tempTable, idColumn, columns}
	err = t.Execute(buf, info)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func Upsert(db *sql.DB, table string, idColumn string, columns []string, rows chan []string) error {
        // Can't create a temporary table inside a non-temporary schema, so just
        // replace the periods, if present, with semicolons to avoid errors.
        tempTable := strings.Replace(table, ".", "_", -1)        

	query, err := buildQuery(table, tempTable, idColumn, columns)
	if err != nil {
		return err
	}

	txn, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = txn.Exec("CREATE TEMP TABLE " + tempTable + "_temp(LIKE " + table + ") ON COMMIT DROP;")
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(tempTable+"_temp", columns...))
	if err != nil {
		return err
	}

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
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	_, err = txn.Exec(query)
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
