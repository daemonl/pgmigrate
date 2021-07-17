package pgmigrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

var shouldLog = os.Getenv("PGMIGRATE_LOG") != ""

type Queryer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func getVersion(ctx context.Context, conn Queryer) (int, error) {
	currentVersionRow := conn.QueryRowContext(ctx, `SELECT version FROM _migrate_`)
	currentVersion := 0
	if err := currentVersionRow.Scan(&currentVersion); err != nil {
		pgErr, ok := err.(*pq.Error)
		if !ok {
			return 0, err
		}
		if pgErr.Code.Name() != "undefined_table" {
			return 0, pgErr
		}
		if _, err = conn.ExecContext(ctx, `
		CREATE TABLE _migrate_ (version int primary key);
		INSERT INTO _migrate_ (version) VALUES (0);
		`); err != nil {
			return 0, err
		}
	}
	return currentVersion, nil
}

func MigrateDatabase(ctx context.Context, conn Queryer, migrationsDir string, targetVersion int) error {

	currentVersion, err := getVersion(ctx, conn)
	if err != nil {
		return err
	}

	if shouldLog {
		log.Printf("Migrate from %d to %d", currentVersion, targetVersion)
	}

	migrateFiles, err := ioutil.ReadDir(migrationsDir)
	if err != nil {
		return err
	}

	upFiles := map[int]string{}
	downFiles := map[int]string{}
	maxMigration := 0

	for _, file := range migrateFiles {
		name := file.Name()
		parts := strings.Split(name, ".")
		if len(parts) != 3 {
			continue
		}
		if parts[2] != "sql" {
			continue
		}

		numberStr := strings.Split(parts[0], "-")[0]
		numberUI64, err := strconv.ParseUint(numberStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid version filename %s", name)
		}
		number := int(numberUI64)

		if maxMigration < number {
			maxMigration = number
		}

		switch parts[1] {
		case "up":
			upFiles[number] = name
		case "down":
			downFiles[number] = name
		default:
			return fmt.Errorf("Bad filename: %s", name)
		}
	}

	for idx := 1; idx < maxMigration; idx++ {
		if _, ok := upFiles[idx]; !ok {
			return fmt.Errorf("Missing Up migration %d", idx)
		}
		if _, ok := downFiles[idx]; !ok {
			return fmt.Errorf("Missing Down migration %d", idx)
		}
	}

	if targetVersion == -1 {
		targetVersion = maxMigration
	}

	if targetVersion > currentVersion {
		for idx := currentVersion + 1; idx <= targetVersion; idx++ {
			if err := runFile(ctx, conn, filepath.Join(migrationsDir, upFiles[idx]), idx); err != nil {
				return err
			}
		}
	} else if targetVersion < currentVersion {
		for idx := currentVersion; idx > targetVersion; idx-- {
			if err := runFile(ctx, conn, filepath.Join(migrationsDir, downFiles[idx]), idx-1); err != nil {
				return err
			}
		}
	}

	return nil
}

func runFile(ctx context.Context, conn Queryer, filename string, version int) error {
	if shouldLog {
		log.Printf("File: %s", filename)
	}
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, string(bytes)); err != nil {
		tx.Rollback() //nolint: errcheck
		if err, ok := err.(*pq.Error); ok {
			log.Printf("PG Error in %s: %s", filename, err.Message)
			if err.Detail != "" {
				log.Printf("Detail: %s", err.Detail)
			}
			if err.Position != "" {
				log.Printf("Position: %s", err.Position)
			}
			if err.Table != "" {
				log.Printf("Table: %s", err.Table)
			}
			if err.Where != "" {
				log.Printf("Where: %s", err.Where)
			}
		}
		return fmt.Errorf("executing %s: %w", filename, err)
	}

	if _, err := conn.ExecContext(ctx, `UPDATE _migrate_ SET version = $1;`, version); err != nil {
		tx.Rollback() //nolint: errcheck
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

type CallbackConnector struct {
	*pq.Connector
	Callback func(context.Context, driver.Conn) error
}

func (tc *CallbackConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := tc.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if err := tc.Callback(ctx, conn); err != nil {
		return nil, err
	}
	return conn, nil
}

func GetTestSchema(testURL string, name string) (*sql.DB, error) {

	connector, err := pq.NewConnector(testURL)
	if err != nil {
		return nil, err
	}

	conn := sql.OpenDB(connector)
	if err != nil {
		return nil, err
	}

	for tries := 0; tries < 30; tries++ {
		err = conn.Ping()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`
		DROP SCHEMA IF EXISTS %s CASCADE;
		CREATE SCHEMA %s;
	`, name, name)); err != nil {
		return nil, err
	}
	conn.Close()

	testConnector := &CallbackConnector{
		Connector: connector,
		Callback: func(ctx context.Context, conn driver.Conn) error {
			execerCtx := conn.(driver.ExecerContext)
			_, err := execerCtx.ExecContext(ctx, fmt.Sprintf("SET search_path TO %s", name), []driver.NamedValue{})
			if err != nil {
				return fmt.Errorf("preparing connection to search_path: %w", err)
			}
			return nil
		},
	}

	return sql.OpenDB(testConnector), nil
}
