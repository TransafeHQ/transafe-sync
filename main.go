package main

import (
	"github.com/TransafeHQ/transafe-sync/internal/sources/db"
)

func main() {

	var source = db.OracleSource{
		Username: "system",
		Password: "welcome123",
		Hostname: "localhost",
		Port:     1521,
		Sid:      "xe",
	}

	var config = db.SyncJobConfig{
		TableName: "FILM",
		Method:    "FULL_EXTRACT",
		ShardSize: 10000,
		Source:    source}

	_, err := db.RunSyncJob(config)
	if err != nil {
		panic(err)
	}
}
