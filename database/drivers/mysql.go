package drivers

import (
	"database/sql"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQL struct represents the configuration for the MySQL database.
type MySQL struct {
	Username  string
	Password  string
	Host      string
	Port      int
	Database  string
	Charset   string
	ParseTime bool
	Loc       string
}

var (
	mysqlClient *gorm.DB
	mysqlDb     *sql.DB
)

// Connect establishes a new connection to the MySQL database.
func (mysqlDriver *MySQL) Connect() (err error) {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=%t&loc=%s",
		mysqlDriver.Username, mysqlDriver.Password, mysqlDriver.Host,
		mysqlDriver.Port, mysqlDriver.Database, mysqlDriver.Charset,
		mysqlDriver.ParseTime, mysqlDriver.Loc,
	)

	mysqlClient, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})

	if err != nil {
		return err
	}

	mysqlDb, _ = mysqlClient.DB()

	return
}

// Close closes the connection to the MySQL database.
func (mysqlDriver *MySQL) Close() (err error) {
	mysqlDb, err = mysqlClient.DB()
	err = mysqlDb.Close()
	if err != nil {
		return err
	}
	return
}

// GetClient returns the gorm.DB mysqlClient for the MySQL database.
func (mysqlDriver *MySQL) GetClient() *gorm.DB {
	return mysqlClient
}

// GetDB returns the sql.DB mysqlClient for the MySQL database.
func (mysqlDriver *MySQL) GetDB() *sql.DB {
	return mysqlDb
}
