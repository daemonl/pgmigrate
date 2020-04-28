package main

import (
	"context"
	"database/sql"
	"flag"
	"log"

	"gopkg.daemonl.com/pgmigrate"
)

func main() {
	pgURL := flag.String("postgres", "", "The Postgres URL")
	targetVersion := flag.Int("target", -1, "The target version. (-1 = latest)")
	migrationsDir := flag.String("migrations", "./migrations", "The migrations source")
	flag.Parse()

	if *pgURL == "" {
		log.Fatal("Requires postgres flag")
	}

	if err := do(*pgURL, *migrationsDir, *targetVersion); err != nil {
		log.Fatal(err.Error())
	}
}

func do(pgURL string, migrationsDir string, targetVersion int) error {

	ctx := context.Background()
	dbPool, err := sql.Open("postgres", pgURL)
	if err != nil {
		return err
	}
	if err := dbPool.Ping(); err != nil {
		return err
	}

	return pgmigrate.MigrateDatabase(ctx, dbPool, migrationsDir, targetVersion)
}
