package main

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

func initDBRetry(dburl string, count int) (*sqlx.DB, error) {
	db, err := initDB(dburl)
	retryCount := count
	for {
		err := db.Ping()
		if err != nil {
			if retryCount == 0 {
				log.Fatal().Msgf("Not able to establish connection to database %s", dburl)
			}

			log.Printf(fmt.Sprintf("Could not connect to database. Wait 2 seconds. %d retries left...", retryCount))
			retryCount--
			time.Sleep(2 * time.Second)
		} else {
			db, err = initDB(dburl)
			if err != nil {
				log.Fatal().Msgf("Not able to establish connection to database %s", dburl)
			}
			break
		}
	}
	return db, err
}

func initDB(dburl string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", dburl)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
CREATE FUNCTION tags_to_tagvalues(jsonb) RETURNS text[]
    AS 'SELECT array_agg(t->>1) FROM (SELECT jsonb_array_elements($1) AS t)s;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE TABLE IF NOT EXISTS event (
  id text NOT NULL,
  pubkey text NOT NULL,
  created_at integer NOT NULL,
  kind integer NOT NULL,
  tags jsonb NOT NULL,
  content text NOT NULL,
  sig text NOT NULL,

  tagvalues text[] GENERATED ALWAYS AS (tags_to_tagvalues(tags)) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS ididx ON event (id);
CREATE UNIQUE INDEX IF NOT EXISTS pubkeytimeidx ON event (pubkey, created_at);
CREATE INDEX IF NOT EXISTS arbitrarytagvalues ON event USING gin (tagvalues);
    `)
	log.Print(err)
	return db, nil
}
