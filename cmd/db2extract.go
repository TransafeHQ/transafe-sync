/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/TransafeHQ/transafe-sync/internal/sources/db"
)

// db2extractCmd represents the db2extract command
var db2extractCmd = &cobra.Command{
	Use:   "db2extract",
	Short: "Extract table (DB2 for LUW)",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("db2extract called")

		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			panic(err)
		}

		tableName, err := cmd.Flags().GetString("table")
		if err != nil {
			panic(err)
		}

		var source = db.OracleSource{
			Username: "system",
			Password: "welcome123",
			Hostname: "localhost",
			Port:     port,
			Sid:      "xe",
		}

		var config = db.SyncJobConfig{
			TableName: tableName,
			Method:    "FULL_EXTRACT",
			ShardSize: 10000,
			Source:    source}

		_, err := db.RunSyncJob(config)
		if err != nil {
			panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(db2extractCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	db2extractCmd.PersistentFlags().String("hostname", "localhost", "A help for foo")
	db2extractCmd.PersistentFlags().Int32("port", 5932, "A help for foo")
	db2extractCmd.PersistentFlags().String("database", "", "A help for foo")

	db2extractCmd.PersistentFlags().String("username", "", "A help for foo")
	db2extractCmd.PersistentFlags().String("password", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	db2extractCmd.Flags().String("schema", "", "Help message for toggle (required)")
	// db2extractCmd.MarkFlagRequired("schema")
	db2extractCmd.Flags().String("table", "", "Help message for toggle (required)")
	db2extractCmd.MarkFlagRequired("table")

	// Parquet Settings
	db2extractCmd.Flags().Int32("shard-size", 1000, "A help for foo")
	db2extractCmd.Flags().String("output", "test.parquet", "A help for foo")
}
