package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/ibmdb/go_ibm_db"
)

type DB2Source struct {
	Username string
	Password string
	Hostname string
	Port     int
	Database string
}

func (s DB2Source) getConnection() (*sql.DB, error) {

	return sql.Open("go_ibm_db", fmt.Sprintf("HOSTNAME=%s;DATABASE=%s;PORT=%s;UID=%s;PWD=%s", s.Hostname, s.Database, s.Port, s.Username, s.Password))
}

// Function to get table columns from the database
func (DB2Source) getColumns(db *sql.DB, tableName string) ([]Column, error) {

	comp := strings.Split(tableName, ".")
	table := comp[1]
	schema := comp[0]

	rows, err := db.Query(fmt.Sprintf("SELECT colname AS column_name, typename AS data_type, nulls AS nullable FROM syscat.columns WHERE tabname = '%s' AND tabschema = '%s' ORDER BY colno ASC", table, schema))
	// rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type, nullable FROM user_tab_columns WHERE table_name = '%s'", strings.ToUpper(tableName)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column

	for rows.Next() {
		var columnName, columnType, nullable string
		err := rows.Scan(&columnName, &columnType, &nullable)
		if err != nil {
			return nil, err
		}

		columns = append(columns, Column{
			Name:       columnName,
			ColumnType: columnType,
			Nullable:   nullable == "Y",
		})

	}

	return columns, nil
}

// Function to convert BD2 data type to Go type
func (DB2Source) getGoType(columnType string) reflect.Type {
	switch strings.ToUpper(columnType) {
	case "NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
		return reflect.TypeOf(float64(0))
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		return reflect.TypeOf("string")
	default:
		return reflect.TypeOf("")
	}
}

// Function to generate a dynamic SQL query with MD5 hash and other columns
func (DB2Source) generateExtractQuery(columns []Column, tableName string) (string, error) {

	var buf bytes.Buffer

	// Construct the SELECT statement
	buf.WriteString("SELECT ")
	// buf.WriteString("STANDARD_HASH(")

	// hash_columns := []string{}
	fields := []string{}
	for _, column := range columns {

		if isExcluded(column.Name) {
			continue
		}

		// hash_columns = append(hash_columns, fmt.Sprintf("TO_CHAR(%s)", column.Name))
		fields = append(fields, column.Name)

		// buf.WriteString("TO_CHAR(")
		// buf.WriteString(column.Name)
		// buf.WriteString(")")
		// if i < len(columns)-1 {
		// 	buf.WriteString(" || ")
		// }
	}
	// buf.WriteString(fmt.Sprintf("%s, 'MD5') AS SYNC_ROW_HASH, ", strings.Join(hash_columns, " || ")))

	// Construct the FROM statement
	buf.WriteString(strings.Join(fields, ","))

	// Add _EXTRACT_DATE column as current timestamp
	buf.WriteString(",SYS_EXTRACT_UTC(CURRENT_TIMESTAMP) AS SYNC_EXTRACT_DATE ")

	// Add FROM
	buf.WriteString(fmt.Sprintf("FROM %s", tableName))

	return buf.String(), nil
}
