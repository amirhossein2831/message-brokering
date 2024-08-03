package database

import (
	"database/sql"
	"github.com/amirhossein2831/message-brokering/database/drivers"
	"gorm.io/gorm"
	"log"
	"os"
	"strconv"
	"sync"
)

// Package-level variables to enforce singleton pattern for database connection and instance retrieval.
var (
	connectOnce     sync.Once // Ensures database connection is established only once.
	getInstanceOnce sync.Once // Ensures a single instance of Database is created.
	instance        *Database // Holds the singleton instance of Database.
)

// IDatabaseDriver defines the interface for database drivers.
// It specifies the methods required for a database driver to be compatible with the Database struct.
type IDatabaseDriver interface {
	Connect() error      // Connect establishes a connection to the database.
	Close() error        // Close terminates the connection to the database.
	GetClient() *gorm.DB // GetClient returns the underlying client for direct operations.
	GetDB() *sql.DB      // GetDB returns the underlying client for direct operations.
}

// Database encapsulates the database operations and driver.
// It serves as a central point for database interactions, leveraging a driver that implements the IDatabaseDriver interface.
type Database struct {
	driver IDatabaseDriver // The database driver, implementing IDatabaseDriver for database operations.
}

// Init initializes the database by establishing a connection.
// It retrieves the singleton instance of the Database and calls Connect on it.
func Init() (err error) {
	return GetInstance().Connect()
}

// Connect establishes a connection to the database if not already connected.
// It uses connectOnce to ensure that the database connection is established only once,
// preventing multiple connections in a concurrent environment.
func (database *Database) Connect() (err error) {
	connectOnce.Do(func() {
		dbPort, _ := strconv.Atoi(os.Getenv("DB_PORT"))
		dbType := os.Getenv("DB_DRIVER")

		switch dbType {
		case "postgres":
			database.driver = &drivers.Postgres{
				Username: os.Getenv("DB_USERNAME"),
				Password: os.Getenv("DB_PASSWORD"),
				Host:     os.Getenv("DB_HOST"),
				Port:     dbPort,
				Database: os.Getenv("DB_DATABASE"),
				SSLMode:  os.Getenv("DB_SSL_MODE"),
				Timezone: os.Getenv("APP_TZ"),
			}
		case "mysql":
			database.driver = &drivers.MySQL{
				Username:  os.Getenv("DB_USERNAME"),
				Password:  os.Getenv("DB_PASSWORD"),
				Host:      os.Getenv("DB_HOST"),
				Port:      dbPort,
				Database:  os.Getenv("DB_DATABASE"),
				Charset:   "utf8mb4",
				ParseTime: true,
				Loc:       os.Getenv("APP_TZ"),
			}
		default:
			log.Fatal("Unsupported database type")
		}

		// Establish connection
		err = database.driver.Connect()
	})
	return
}

// Close terminates the database connection.
// It delegates the close operation to the database driver and logs the closure.
func (database *Database) Close() (err error) {
	err = database.driver.Close()
	log.Println("Database Service: Disconnected Successfully.")
	return
}

// GetClient retrieves the gorm.DB client from the database driver.
// It allows for direct database operations using the ORM.
func (database *Database) GetClient() *gorm.DB {
	return database.driver.GetClient()
}

// GetDB retrieves the sql.DB client from the database driver.
// It allows for direct database operations using the ORM.
func (database *Database) GetDB() *sql.DB {
	return database.driver.GetDB()
}

// GetInstance returns the singleton instance of the Database.
// It ensures that only one instance of Database is created and used throughout the application,
// leveraging getInstanceOnce to enforce this constraint.
func GetInstance() *Database {
	getInstanceOnce.Do(func() {
		instance = &Database{} // Initialize the singleton instance
	})
	return instance
}
