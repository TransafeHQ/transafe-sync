package db

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

type DatabaseSink struct {
	Name          string
	Database      string
	ContectionStr string
	Destination   string
}

type SyncJobConfig struct {
	TableName string
	Method    string
	ShardSize int
	Source    Source
}

type Column struct {
	Name       string
	ColumnType string
	Nullable   bool
}

type Source interface {
	getConnection() (*sql.DB, error)
	getColumns(db *sql.DB, tableName string) ([]Column, error)
	getGoType(columnType string) reflect.Type
	generateExtractQuery(columns []Column, tableName string) (string, error)
}

// Function that returns a function to determine of a column needs to be excluded
func isExcluded(columnName string) bool {
	exclusions := []string{"SYNC_EXTRACT_DATE", "SYNC_ROW_HASH"}

	for _, exclusion := range exclusions {
		if strings.Contains(columnName, exclusion) {
			return true
		}
	}
	return false
}

func dieOnError(msg string, err error) {
	if err != nil {
		fmt.Println(msg, err)
		os.Exit(1)
	}
}

func RunSyncJob(job SyncJobConfig) (string, error) {

	// Connect to the Source
	conn, err := job.Source.getConnection()
	dieOnError("Unable to connect", err)

	defer conn.Close()

	fmt.Println("Successfully connected.")

	// Get database columns
	columns, err := job.Source.getColumns(conn, job.TableName)
	dieOnError("Unable to get table columns", err)
	columns = append(columns,
		// Column{
		// 	Name:       "SYNC_ROW_HASH",
		// 	ColumnType: "string",
		// 	Nullable:   false,
		// },
		Column{
			Name:       "SYNC_EXTRACT_DATE",
			ColumnType: "TIMESTAMP",
			Nullable:   false,
		})

	// Generate schema
	// Create a struct type dynamically based on table columns
	querySchema := reflect.StructOf(getSchemaFromColumns(columns, job.Source))

	// Run extract query
	query, err := job.Source.generateExtractQuery(columns, job.TableName)
	dieOnError("Can't generate query:", err)
	fmt.Println(query)

	rows, err := conn.Query(query)
	dieOnError("Can't create query:", err)

	defer rows.Close()

	// Generate file format
	// open output file
	fo, err := os.Create(fmt.Sprintf("%s.parquet", job.TableName))
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	writer := parquet.NewWriter(fo, parquet.ColumnPageBuffers(parquet.NewFileBufferPool("", "buffers.*")))

	for rows.Next() {
		// Create a new instance of the dynamic struct
		data := reflect.New(querySchema).Interface()

		// Scan the row into the dynamic struct
		err := rows.Scan(sliceToInterface(data)...)
		if err != nil {
			dieOnError("Error scanning row:", err)
		}

		err = writer.Write(data)
		if err != nil {
			dieOnError("Error writing to Parquet file:", err)
		}
	}

	if rows.Err() != nil && rows.Err() != io.EOF {
		dieOnError("Can't fetch row:", rows.Err())
	}

	_ = writer.Close()

	// Upload to destination
	return job.Method, nil
}

// Function to get table columns from the database
func getSchemaFromColumns(columns []Column, source Source) []reflect.StructField {

	var schema []reflect.StructField

	for _, column := range columns {

		// Convert Oracle data type to Go type
		goType := source.getGoType(column.ColumnType)
		println(fmt.Sprintf("%s =  %s", column.Name, goType))

		// Determine the type of the struct field based on the nullable status
		var fieldType reflect.Type
		if column.Nullable {
			fieldType = reflect.PointerTo(goType) // If nullable, use a pointer to the Go type
		} else {
			fieldType = goType // If not nullable, use the Go type directly
		}

		// Create a struct field based on the column name and type
		field := reflect.StructField{
			Name: strings.ToUpper(column.Name),
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"name=%s, type=%s"`, column.Name, goType.Name())),
		}

		schema = append(schema, field)
	}

	return schema
}

// Function to convert slice to interface slice
func sliceToInterface(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Ptr {
		panic("sliceToInterface: not a pointer")
	}
	s = s.Elem()
	if s.Kind() != reflect.Struct {
		panic("sliceToInterface: not a struct")
	}

	result := make([]interface{}, s.NumField())
	for i := 0; i < s.NumField(); i++ {
		result[i] = s.Field(i).Addr().Interface()
	}
	return result
}
