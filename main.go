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



func main() {
	// Initialize BigQuery client
	ctx := context.Background()
	var err error

	serviceAccountPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if serviceAccountPath == "" {
		if os.Getenv("ENV") == "production" {
			// Production path for new service account
			serviceAccountPath = "/shared/volumes/a21e22/golang-api-bigquery.json"
		} else {
			// Default to local path for development
			serviceAccountPath = "./golang-api-bigquery.json"
		}
	}

	fmt.Printf("Using service account file: %s\n", serviceAccountPath)
	
	// Check if file exists and read content
	if _, err := os.Stat(serviceAccountPath); os.IsNotExist(err) {
		panic(fmt.Sprintf("Service account file does not exist at: %s", serviceAccountPath))
	}
	
	// Read and log service account file content
	fileContent, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read service account file: %v", err))
	}
	
	fmt.Printf("Service account file size: %d bytes\n", len(fileContent))
	
	// Parse JSON to extract key info
	var serviceAccount map[string]interface{}
	if err := json.Unmarshal(fileContent, &serviceAccount); err != nil {
		fmt.Printf("Failed to parse service account JSON: %v\n", err)
		fmt.Printf("File content preview: %s\n", string(fileContent[:min(200, len(fileContent))]))
	} else {
		if clientEmail, ok := serviceAccount["client_email"].(string); ok {
			fmt.Printf("Service account email: %s\n", clientEmail)
		}
		if projectID, ok := serviceAccount["project_id"].(string); ok {
			fmt.Printf("Service account project: %s\n", projectID)
		}
		if accountType, ok := serviceAccount["type"].(string); ok {
			fmt.Printf("Account type: %s\n", accountType)
		}
	}
	
	bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", 
		option.WithCredentialsFile(serviceAccountPath))

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
