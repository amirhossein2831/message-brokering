package drivers

import (
	"database/sql"
	"fmt"
	gormPsql "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Postgres struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	SSLMode  string
	Timezone string
}

var (
	postgresClient *gorm.DB
	postgresDb     *sql.DB
)

// Connect establishes new connection to database.
func (postgres *Postgres) Connect() (err error) {

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		postgres.Host, postgres.Port, postgres.Username,
		postgres.Password, postgres.Database, postgres.SSLMode, postgres.Timezone,
	)

	postgresClient, err = gorm.Open(gormPsql.Open(dsn))

	if err != nil {
		return err
	}

	postgresDb, _ = postgresClient.DB()

	return
}

// Close closes the connection to database.
func (postgres *Postgres) Close() (err error) {
	postgresDb, err = postgresClient.DB()
	err = postgresDb.Close()
	if err != nil {
		return err
	}
	return
}

// GetClient returns an instance of database.
func (postgres *Postgres) GetClient() *gorm.DB {
	return postgresClient
}

// GetDB returns an instance of database.
func (postgres *Postgres) GetDB() *sql.DB {
	return postgresDb
}
