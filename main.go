// main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var (
	bqClient *bigquery.Client
)

type OrderItem struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Order struct {
	EstimatedDeliveryDate string      `json:"estimated_delivery_date"`
	Items                 []OrderItem `json:"items"`
}

type BigQueryOrderItem struct {
	ID          int64  `bigquery:"id"`
	DeliveryDate string `bigquery:"delivery_date"`
	ProductID   int64  `bigquery:"product_id"`
	SKU         string `bigquery:"sku"`
	Size        string `bigquery:"size"`
	Quantity    int64  `bigquery:"quantity"`
}

func createCredentialsFromEnv() (option.ClientOption, error) {
	// Check if all required environment variables are present
	projectID := os.Getenv("GOOGLE_PROJECT_ID")
	privateKeyID := os.Getenv("GOOGLE_PRIVATE_KEY_ID")
	privateKey := os.Getenv("GOOGLE_PRIVATE_KEY")
	clientEmail := os.Getenv("GOOGLE_CLIENT_EMAIL")
	clientID := os.Getenv("GOOGLE_CLIENT_ID")

	if projectID == "" || privateKeyID == "" || privateKey == "" || clientEmail == "" || clientID == "" {
		return nil, fmt.Errorf("missing required environment variables for service account")
	}

	// Create service account JSON from environment variables
	serviceAccountJSON := map[string]string{
		"type":                        "service_account",
		"project_id":                  projectID,
		"private_key_id":              privateKeyID,
		"private_key":                 privateKey,
		"client_email":                clientEmail,
		"client_id":                   clientID,
		"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
		"token_uri":                   "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url":        fmt.Sprintf("https://www.googleapis.com/robot/v1/metadata/x509/%s", clientEmail),
		"universe_domain":             "googleapis.com",
	}

	// Convert to JSON bytes
	jsonBytes, err := json.Marshal(serviceAccountJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal service account JSON: %v", err)
	}

	// Create credentials from JSON
	creds, err := google.CredentialsFromJSON(context.Background(), jsonBytes, "https://www.googleapis.com/auth/bigquery")
	if err != nil {
		return nil, fmt.Errorf("failed to create credentials from JSON: %v", err)
	}

	return option.WithCredentials(creds), nil
}

func main() {
	// Initialize BigQuery client
	ctx := context.Background()
	var err error

	// Check if we should force ADC
	forceADC := os.Getenv("FORCE_ADC") == "true"
	
	serviceAccountPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if serviceAccountPath == "" && !forceADC {
		if os.Getenv("ENV") == "production" {
			// Production path - same as working JS API
			serviceAccountPath = "/shared/volumes/a9e10/service_account_dbt.json"
		} else {
			// Default to local path for development
			serviceAccountPath = "/Users/roberto/Code/Partners/Etiql/service_accounts/service_account_dbt.json"
		}
	}

	if forceADC {
		fmt.Println("Using Application Default Credentials (FORCE_ADC=true)")
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307",
			option.WithScopes("https://www.googleapis.com/auth/bigquery"))
		if err != nil {
			panic(fmt.Sprintf("Failed to create BigQuery client with ADC: %v", err))
		}
		fmt.Println("BigQuery client created successfully with ADC")
	} else if credOption, err := createCredentialsFromEnv(); err == nil {
		fmt.Printf("Using environment variables for service account: %s\n", os.Getenv("GOOGLE_CLIENT_EMAIL"))
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", credOption)
		if err != nil {
			panic(fmt.Sprintf("Failed to create BigQuery client with env credentials: %v", err))
		}
		fmt.Println("BigQuery client created successfully with env vars")
	} else {
		fmt.Printf("Using service account file: %s\n", serviceAccountPath)
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", 
			option.WithCredentialsFile(serviceAccountPath))
	}

	if err != nil {
		panic(fmt.Sprintf("Failed to create BigQuery client: %v", err))
	}
	fmt.Println("BigQuery client initialized successfully")
	defer bqClient.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Server is running"})
	})

	router.GET("/test-connection", testBigQueryConnection)
	router.GET("/purchase-orders", getPurchaseOrders)

	if os.Getenv("ENV") == "production" {
		fmt.Printf("Starting server in production mode on port %s\n", os.Getenv("PORT"))
		if err := router.Run(fmt.Sprintf(":%s", os.Getenv("PORT"))); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	} else {
		fmt.Println("Starting server in development mode on port 8011")
		if err := router.Run(":8011"); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	}
}

func testBigQueryConnection(c *gin.Context) {
	fmt.Println("=== Testing BigQuery Connection ===")
	ctx := context.Background()

	// Test 1: Simple query
	fmt.Println("Test 1: Basic SELECT query")
	query1 := bqClient.Query("SELECT 1 as test_value")
	it1, err := query1.Read(ctx)
	if err != nil {
		fmt.Printf("Test 1 FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "basic_query",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	
	var result1 struct {
		TestValue int64 `bigquery:"test_value"`
	}
	err = it1.Next(&result1)
	if err != nil {
		fmt.Printf("Test 1 result FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "basic_query_result",
			"status": "failed", 
			"error": err.Error(),
		})
		return
	}
	fmt.Printf("Test 1 SUCCESS: %d\n", result1.TestValue)

	// Test 2: Project access
	fmt.Println("Test 2: Project datasets access")
	query2 := bqClient.Query("SELECT COUNT(*) as dataset_count FROM `metal-force-400307.INFORMATION_SCHEMA.SCHEMATA`")
	it2, err := query2.Read(ctx)
	if err != nil {
		fmt.Printf("Test 2 FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "project_access",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	
	var result2 struct {
		DatasetCount int64 `bigquery:"dataset_count"`
	}
	err = it2.Next(&result2)
	if err != nil {
		fmt.Printf("Test 2 result FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "project_access_result",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	fmt.Printf("Test 2 SUCCESS: Found %d datasets\n", result2.DatasetCount)

	// Test 3: Agent dataset access
	fmt.Println("Test 3: Agent dataset access")
	query3 := bqClient.Query("SELECT COUNT(*) as table_count FROM `metal-force-400307.agent.INFORMATION_SCHEMA.TABLES`")
	it3, err := query3.Read(ctx)
	if err != nil {
		fmt.Printf("Test 3 FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "agent_dataset",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	
	var result3 struct {
		TableCount int64 `bigquery:"table_count"`
	}
	err = it3.Next(&result3)
	if err != nil {
		fmt.Printf("Test 3 result FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "agent_dataset_result",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	fmt.Printf("Test 3 SUCCESS: Found %d tables in agent dataset\n", result3.TableCount)

	// Test 4: Purchase orders table access
	fmt.Println("Test 4: Purchase orders table access")
	query4 := bqClient.Query("SELECT COUNT(*) as row_count FROM `metal-force-400307.agent.purchase_orders` LIMIT 1")
	it4, err := query4.Read(ctx)
	if err != nil {
		fmt.Printf("Test 4 FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "purchase_orders_table",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	
	var result4 struct {
		RowCount int64 `bigquery:"row_count"`
	}
	err = it4.Next(&result4)
	if err != nil {
		fmt.Printf("Test 4 result FAILED: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"test": "purchase_orders_table_result",
			"status": "failed",
			"error": err.Error(),
		})
		return
	}
	fmt.Printf("Test 4 SUCCESS: Found %d rows in purchase_orders table\n", result4.RowCount)

	fmt.Println("=== ALL TESTS PASSED ===")
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"message": "All BigQuery connection tests passed",
		"results": gin.H{
			"basic_query": result1.TestValue,
			"datasets_count": result2.DatasetCount,
			"agent_tables_count": result3.TableCount,
			"purchase_orders_rows": result4.RowCount,
		},
	})
}

func getPurchaseOrders(c *gin.Context) {
	fmt.Println("=== Purchase Orders API called ===")
	ctx := context.Background()

	query := bqClient.Query(`
		SELECT 
			id,
			delivery_date,
			items.product_id,
			items.sku,
			items.size,
			items.quantity
		FROM metal-force-400307.agent.purchase_orders,
		UNNEST(items) as items
		WHERE delivery_date >= FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())
		ORDER BY delivery_date, id, items.product_id
	`)

	fmt.Println("Executing BigQuery query...")
	it, err := query.Read(ctx)
	if err != nil {
		fmt.Printf("BigQuery ERROR: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to query BigQuery",
			"details": err.Error(),
		})
		return
	}
	fmt.Println("BigQuery query executed successfully")

		// Group items by order ID
	orderMap := make(map[int64]map[string][]BigQueryOrderItem)
	itemCount := 0

	for {
		var item BigQueryOrderItem
		err := it.Next(&item)
		if err != nil {
			break
		}
		itemCount++

		if orderMap[item.ID] == nil {
			orderMap[item.ID] = make(map[string][]BigQueryOrderItem)
		}
		if orderMap[item.ID][item.DeliveryDate] == nil {
			orderMap[item.ID][item.DeliveryDate] = []BigQueryOrderItem{}
		}
		orderMap[item.ID][item.DeliveryDate] = append(orderMap[item.ID][item.DeliveryDate], item)
	}

	fmt.Printf("Processed %d items, found %d orders\n", itemCount, len(orderMap))

	response := make(map[string]Order)

	// Transform grouped data into orders
	for orderID, deliveryDates := range orderMap {
		for deliveryDate, items := range deliveryDates {
			transformedOrder := transformOrderFromItems(orderID, deliveryDate, items)
			if transformedOrder != nil {
				orderKey := fmt.Sprintf("order_%d", orderID)
				response[orderKey] = *transformedOrder
			}
		}
	}

	fmt.Printf("Returning %d orders in response\n", len(response))
	c.Header("Cache-Control", "private, max-age=300")
	c.JSON(http.StatusOK, response)
}

func transformOrderFromItems(orderID int64, deliveryDate string, items []BigQueryOrderItem) *Order {
	if deliveryDate == "" || len(items) == 0 {
		return nil
	}

	// Parse delivery date to ensure it's valid
	if _, err := time.Parse("2006-01-02", deliveryDate); err != nil {
		return nil
	}

	var orderItems []OrderItem
	for _, item := range items {
		if item.Quantity > 0 && item.ProductID > 0 {
			orderItems = append(orderItems, OrderItem{
				SKU:      fmt.Sprintf("ETIQL%d", item.ProductID),
				Quantity: int(item.Quantity),
			})
		}
	}

	if len(orderItems) == 0 {
		return nil
	}

	return &Order{
		EstimatedDeliveryDate: deliveryDate,
		Items:                 orderItems,
	}
}
