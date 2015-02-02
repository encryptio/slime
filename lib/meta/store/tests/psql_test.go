package tests

import (
	"os"
	"testing"

	"git.encryptio.com/slime/lib/meta/store/psql"
)

func TestPSQLDriver(t *testing.T) {
	dsn := os.Getenv("PSQL_DSN")
	if dsn == "" {
		t.Skip("Set PSQL_DSN to enable PostgreSQL tests")
	}

	s, err := psql.Open(dsn)
	if err != nil {
		t.Fatalf("Couldn't open psql driver: %v", err)
	}
	defer s.Close()

	testShuffleShardedIncrement(t, s)
	testRangeMaxRandomReplacement(t, s)
}
