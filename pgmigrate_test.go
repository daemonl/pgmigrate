package pgmigrate

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var s1u string = `CREATE TABLE foo (id int);`
var s1d string = `DROP TABLE foo;`
var s2u string = `CREATE TABLE bar (id int);`
var s2d string = `DROP TABLE bar`
var s3u string = `CREATE TABLE baz (id int);`
var s3d string = `DROP TABLE baz;`

func TestMigrate(t *testing.T) {
	migrateDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	defer os.RemoveAll(migrateDir)

	for key, content := range map[string]string{
		"001-foo.up.sql":   s1u,
		"001-foo.down.sql": s1d,
		"002-bar.up.sql":   s2u,
		"002-bar.down.sql": s2d,
		"003-baz.up.sql":   s3u,
		"003-baz.down.sql": s3d,
	} {
		if err := ioutil.WriteFile(filepath.Join(migrateDir, key), []byte(content), 0660); err != nil {
			t.Fatal(err.Error())
		}
	}

	testURL := os.Getenv("TEST_DB")
	if !strings.Contains(testURL, "test") {
		t.Fatalf("Not a test URL: %s", testURL)
	}

	conn, err := GetTestSchema(testURL)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()
	defer conn.Close()

	assertVersion := func(expect int) {
		t.Helper()

		if v, err := getVersion(ctx, conn); err != nil {
			t.Fatalf("Expected no error getting version: %s", err.Error())
		} else if v != expect {
			t.Fatalf("Wrong version %d (expected %d)", v, expect)
		}
	}

	assertVersion(0)
	assertVersion(0) // runs a different code path

	if err := MigrateDatabase(ctx, conn, migrateDir, 2); err != nil {
		t.Fatalf("Unable to migrate: %s", err.Error())
	}

	assertVersion(2)

	if err := MigrateDatabase(ctx, conn, migrateDir, 1); err != nil {
		t.Fatalf("Unable to migrate: %s", err.Error())
	}

	assertVersion(1)

	if err := MigrateDatabase(ctx, conn, migrateDir, -1); err != nil {
		t.Fatalf("Unable to migrate: %s", err.Error())
	}

	assertVersion(3)

}
