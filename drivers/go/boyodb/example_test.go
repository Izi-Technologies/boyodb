package boyodb_test

import (
	"fmt"
	"log"

	"github.com/loreste/boyodb/drivers/go/boyodb"
)

func Example_basicQuery() {
	// Connect to the server
	client, err := boyodb.NewClient("localhost:8765", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Execute a query
	result, err := client.Query("SELECT id, name, email FROM users LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()

	// Iterate over results
	for result.Next() {
		var id int64
		var name, email string
		if err := result.Scan(&id, &name, &email); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("User: id=%d, name=%s, email=%s\n", id, name, email)
	}
}

func Example_withAuthentication() {
	// Connect with TLS and authentication
	config := &boyodb.Config{
		TLS:          true,
		Token:        "your-auth-token",
		QueryTimeout: 60000, // 60 seconds
	}

	client, err := boyodb.NewClient("localhost:8765", config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Or login with username/password
	if err := client.Login("admin", "password"); err != nil {
		log.Fatal(err)
	}
	defer client.Logout()

	// Now execute authenticated queries
	result, err := client.Query("SELECT * FROM sensitive_data")
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()
	// ... process results
}

func Example_createDatabase() {
	client, err := boyodb.NewClient("localhost:8765", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create a new database
	if err := client.CreateDatabase("analytics"); err != nil {
		log.Fatal(err)
	}

	// Create a table
	if err := client.CreateTable("analytics", "events"); err != nil {
		log.Fatal(err)
	}

	// List all databases
	dbs, err := client.ListDatabases()
	if err != nil {
		log.Fatal(err)
	}
	for _, db := range dbs {
		fmt.Println("Database:", db)
	}
}

func Example_ingestCSV() {
	client, err := boyodb.NewClient("localhost:8765", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// CSV data to ingest
	csvData := []byte(`id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,charlie@example.com
`)

	// Ingest into a table
	err = client.IngestCSV("mydb", "users", csvData, true, ",")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("CSV data ingested successfully")
}

func Example_scanToMap() {
	client, err := boyodb.NewClient("localhost:8765", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.Query("SELECT * FROM users LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()

	// Scan results to a slice of maps
	rows, err := result.ToSlice()
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("Row: %v\n", row)
	}
}

func Example_explainQuery() {
	client, err := boyodb.NewClient("localhost:8765", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Get query execution plan
	plan, err := client.Explain("SELECT * FROM users WHERE id > 100")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Query Plan:", plan)
}

func Example_metrics() {
	client, err := boyodb.NewClient("localhost:8765", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Get server metrics
	metrics, err := client.Metrics()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Server Metrics:", metrics)
}
