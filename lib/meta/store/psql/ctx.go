package psql

import (
	"database/sql"
	"fmt"

	"git.encryptio.com/slime/lib/meta/store"

	"github.com/lib/pq"
)

type ctx struct {
	sqlTx      *sql.Tx
	needsRetry bool
}

func (c *ctx) checkErr(err error) {
	if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "40001" {
		// concurrent update, abort transaction and retry
		c.needsRetry = true
	}
}

func (c *ctx) Get(key []byte) (store.Pair, error) {
	var p store.Pair

	row := c.sqlTx.QueryRow("SELECT key, value FROM data WHERE key = $1", key)
	err := row.Scan(&p.Key, &p.Value)
	if err != nil {
		c.checkErr(err)
		if err == sql.ErrNoRows {
			return p, store.ErrNotFound
		}
		return p, err
	}

	return p, nil
}

func (c *ctx) Set(p store.Pair) error {
	// Upsert
	_, err := c.sqlTx.Exec(
		"WITH "+
			"upsert AS ("+
			"    UPDATE data SET value = $2 WHERE key = $1 RETURNING *"+
			") "+
			"INSERT INTO data (key, value) SELECT $1, $2 "+
			"    WHERE NOT EXISTS (SELECT * FROM upsert)", p.Key, p.Value)
	if err != nil {
		c.checkErr(err)
		return err
	}

	return nil
}

func (c *ctx) Delete(key []byte) error {
	res, err := c.sqlTx.Exec("DELETE FROM data WHERE key = $1", key)
	if err != nil {
		c.checkErr(err)
		return err
	}

	count, err := res.RowsAffected()
	if err != nil {
		c.checkErr(err)
		return err
	}

	if count == 0 {
		return store.ErrNotFound
	}

	return nil
}

func (c *ctx) Range(low, high []byte, limit int) ([]store.Pair, error) {
	params := make([]interface{}, 0, 2)
	query := "SELECT key, value FROM data WHERE TRUE"
	if low != nil {
		query += fmt.Sprintf(" AND key >= $%v", len(params)+1)
		params = append(params, low)
	}
	if high != nil {
		query += fmt.Sprintf(" AND KEY < $%v", len(params)+1)
		params = append(params, high)
	}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %v", limit)
	}

	rows, err := c.sqlTx.Query(query, params...)
	if err != nil {
		c.checkErr(err)
		return nil, err
	}
	defer rows.Close()

	pairs := make([]store.Pair, 0, 16)
	for rows.Next() {
		var k, v []byte
		err = rows.Scan(&k, &v)
		if err != nil {
			c.checkErr(err)
			return nil, err
		}

		pairs = append(pairs, store.Pair{k, v}) // TODO: needs copy?
	}
	err = rows.Err()
	if err != nil {
		c.checkErr(err)
		return nil, err
	}

	return pairs, nil
}
