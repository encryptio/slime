package psql

import (
	"database/sql"

	"git.encryptio.com/slime/lib/meta/store"
)

type Store struct {
	sqlDB *sql.DB
}

func Open(dsn string) (*Store, error) {
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	err = sqlDB.Ping()
	if err != nil {
		sqlDB.Close()
		return nil, err
	}

	s := &Store{sqlDB: sqlDB}

	err = s.ensureTable()
	if err != nil {
		sqlDB.Close()
		return nil, err
	}

	return s, nil
}

func (s *Store) Close() {
	s.sqlDB.Close()
}

func (s *Store) ensureTable() error {
	_, err := s.sqlDB.Exec(
		"CREATE TABLE IF NOT EXISTS " +
			"data (" +
			"    key bytea not null primary key," +
			"    value bytea not null" +
			") " +
			"WITH ( OIDS=FALSE, fillfactor=90 )")
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) RunTx(tx store.Tx) (interface{}, error) {
	for {
		ret, err, needsRetry := s.tryTx(tx)
		if needsRetry {
			continue
		}
		return ret, err
	}
}

func (s *Store) tryTx(tx store.Tx) (interface{}, error, bool) {
	sqlTx, err := s.sqlDB.Begin()
	if err != nil {
		return nil, err, false
	}

	_, err = sqlTx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	if err != nil {
		return nil, err, false
	}

	ctx := &ctx{sqlTx: sqlTx}

	ret, err := tx(ctx)
	if err != nil {
		ctx.checkErr(err)
		err2 := sqlTx.Rollback()
		ctx.checkErr(err2)
		// err2 is not returned; the first error is probably more important
		return nil, err, ctx.needsRetry
	}

	err = sqlTx.Commit()
	if err != nil {
		ctx.checkErr(err)
		return nil, err, ctx.needsRetry
	}

	return ret, nil, ctx.needsRetry
}
