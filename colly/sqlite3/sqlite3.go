package sqlite3

import (
	"fmt"
	"log"

	// "database/sql"

	"net/url"
	"sync"

	// _ "github.com/mattn/go-sqlite3"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"strings"
)

// Storage implements a SQLite3 storage backend for Colly
type Storage struct {
	// Filename indicates the name of the sqlite file to use
	Filename string
	// handle to the db
	dbh *gorm.DB
	mu  sync.RWMutex // Only used for cookie methods.
}

// Init initializes the sqlite3 storage
func (s *Storage) Init() error {

	if s.dbh == nil {
		db, err := gorm.Open(sqlite.Open(s.Filename))
		if err != nil {
			return fmt.Errorf("unable to open db file: %s", err.Error())
		}
		s.dbh = db
	}
	// create the data structures if necessary
	statement := s.dbh.Exec("CREATE TABLE IF NOT EXISTS visited (id INTEGER PRIMARY KEY, requestID INTEGER, visited INT)")
	if statement.Error != nil {
		return statement.Error
	}
	statement = s.dbh.Exec("CREATE INDEX IF NOT EXISTS idx_visited ON visited (requestID)")
	if statement.Error != nil {
		return statement.Error
	}
	statement = s.dbh.Exec("CREATE TABLE IF NOT EXISTS cookies (id INTEGER PRIMARY KEY, host TEXT, cookies TEXT)")
	if statement.Error != nil {
		return statement.Error
	}
	statement = s.dbh.Exec("CREATE INDEX IF NOT EXISTS idx_cookies ON cookies (host)")
	if statement.Error != nil {
		return statement.Error
	}
	statement = s.dbh.Exec("CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, data BLOB)")
	if statement.Error != nil {
		return statement.Error
	}
	return nil
}

// Clear removes all entries from the storage
func (s *Storage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	statement := s.dbh.Exec("DROP TABLE visited")
	if statement.Error != nil {
		return statement.Error
	}
	statement = s.dbh.Exec("DROP TABLE cookies")
	if statement.Error != nil {
		return statement.Error
	}

	statement = s.dbh.Exec("DROP TABLE queue")
	if statement.Error != nil {
		return statement.Error
	}
	return nil
}

// Close the db
func (s *Storage) Close() error {
	db, err := s.dbh.DB()
	if err := db.Close(); err != nil {
		return err
	}
	return err
}

// Visited implements colly/storage.Visited()
func (s *Storage) Visited(requestID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	statement := s.dbh.Exec("INSERT INTO visited (requestID, visited) VALUES (?, 1)", requestID)
	if statement.Error != nil {
		return statement.Error
	}
	return nil
}

// IsVisited implements colly/storage.IsVisited()
func (s *Storage) IsVisited(requestID int64) (bool, error) {
	var count int
	statement := s.dbh.Select("SELECT COUNT(*) FROM visited where requestId = ?", requestID)
	// [golang/go/issues/6113] we can't use uint64 with the high bit set
	// but we can cast it and store as an int64 without data loss
	if statement.Error != nil {
		return false, statement.Error
	}
	row := statement.Row()
	if row == nil {
		return false, nil
	}
	err := row.Scan(&count)
	if err != nil {
		return false, err
	}
	if count >= 1 {
		return true, nil
	}
	return false, nil
}

// SetCookies implements colly/storage..SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	// TODO Cookie methods currently have no way to return an error.

	// We need to use a write lock to prevent a race in the db:
	// if two callers set cookies in a very small window of time,
	// it is possible to drop the new cookies from one caller
	// ('last update wins' == best avoided).
	s.mu.Lock()
	defer s.mu.Unlock()

	statement := s.dbh.Exec("INSERT INTO cookies (host, cookies) VALUES (?,?)", u.Host, cookies)
	if statement.Error != nil {
		log.Printf("SetCookies() .Set error %s", statement.Error)
	}

}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	// TODO Cookie methods currently have no way to return an error.
	var cookies string
	s.mu.RLock()

	//cookiesStr, err := s.Client.Get(s.getCookieID(u.Host)).Result()
	statement := s.dbh.Find("SELECT cookies FROM cookies where host = ?", u.Host)

	err := statement.Scan(&cookies)

	s.mu.RUnlock()

	if err != nil {
		if strings.Contains(statement.Error.Error(), "no rows") {
			return ""
		}

		log.Printf("Cookies() .Get error %s", err.Error.Error())
	}

	return cookies
}

// AddRequest implements queue.Storage.AddRequest() function
func (s *Storage) AddRequest(r []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	//return s.Client.RPush(s.getQueueID(), r).Err()
	statement := s.dbh.Exec("INSERT INTO queue (data) VALUES (?)", string(r))
	if statement.Error != nil {
		return statement.Error
	}
	return nil
}

// GetRequest implements queue.Storage.GetRequest() function
func (s *Storage) GetRequest() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var blob []byte
	var id int
	row := s.dbh.Table("queue").Select("min(id), data").Row()
	err := row.Scan(&id, &blob)
	if err != nil {
		return nil, err
	}

	statement := s.dbh.Exec("DELETE FROM queue where id = ?", id)
	if statement.Error != nil {
		return nil, statement.Error
	}

	return blob, nil
}

// QueueSize implements queue.Storage.QueueSize() function
func (s *Storage) QueueSize() (int, error) {
	var count int64

	err := s.dbh.Table("queue").Count(&count).Error
	if err != nil {
		return 0, err
	}
	return int(count), nil
}
