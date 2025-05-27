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
			// Production path - same as working JS API
			serviceAccountPath = "/shared/volumes/a9e10/service_account_dbt.json"
		} else {
			// Default to local path for development
			serviceAccountPath = "/Users/roberto/Code/Partners/Etiql/service_accounts/service_account_dbt.json"
		}
	}

	fmt.Printf("Using service account file: %s\n", serviceAccountPath)
	
	// Check if service account file exists and read some info
	if _, err := os.Stat(serviceAccountPath); os.IsNotExist(err) {
		fmt.Printf("WARNING: Service account file does not exist at: %s\n", serviceAccountPath)
	} else {
		fmt.Printf("Service account file found at: %s\n", serviceAccountPath)
		
		// Try to read the service account file to get the client email
		if fileContent, err := os.ReadFile(serviceAccountPath); err == nil {
			// Parse JSON to extract client_email
			var serviceAccount map[string]interface{}
			if err := json.Unmarshal(fileContent, &serviceAccount); err == nil {
				if clientEmail, ok := serviceAccount["client_email"].(string); ok {
					fmt.Printf("Service account email: %s\n", clientEmail)
				} else {
					fmt.Println("Could not find client_email in service account file")
				}
				if projectID, ok := serviceAccount["project_id"].(string); ok {
					fmt.Printf("Service account project_id: %s\n", projectID)
				}
			} else {
				fmt.Printf("Could not parse service account JSON: %v\n", err)
			}
		} else {
			fmt.Printf("Could not read service account file: %v\n", err)
		}
	}

	// Try to create BigQuery client with service account file first
	bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", option.WithCredentialsFile(serviceAccountPath))

	if err != nil {
		fmt.Printf("Failed to create BigQuery client with service account file: %v\n", err)
		fmt.Println("Attempting to use Application Default Credentials...")
		
		// Fallback to Application Default Credentials
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307")
		if err != nil {
			panic(fmt.Sprintf("Failed to create BigQuery client with both service account and ADC: %v", err))
		}
		fmt.Println("BigQuery client created successfully using Application Default Credentials")
	} else {
		fmt.Println("BigQuery client created successfully using service account file")
	}
	defer bqClient.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Server is running"})
	})

	router.GET("/test-bigquery", testBigQuery)
	router.GET("/purchase-orders", getPurchaseOrders)

	if os.Getenv("ENV") == "production" {
		fmt.Println("Running in production mode on port", os.Getenv("PORT"))
		if err := router.Run(fmt.Sprintf(":%s", os.Getenv("PORT"))); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	} else {
		fmt.Println("Running in development mode on port 8011")
		if err := router.Run(":8011"); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	}
}

func testBigQuery(c *gin.Context) {
	fmt.Println("=== Testing BigQuery access ===")
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{"Basic query", "SELECT 1 as test_value"},
		{"Project access", "SELECT COUNT(*) as dataset_count FROM `metal-force-400307.INFORMATION_SCHEMA.SCHEMATA`"},
		{"Agent dataset", "SELECT COUNT(*) as table_count FROM `metal-force-400307.agent.INFORMATION_SCHEMA.TABLES`"},
		{"Purchase orders table", "SELECT COUNT(*) as row_count FROM `metal-force-400307.agent.purchase_orders` LIMIT 1"},
		{"Purchase orders simple", "SELECT id, delivery_date FROM `metal-force-400307.agent.purchase_orders` LIMIT 1"},
		{"Purchase orders with items", "SELECT id, delivery_date, items FROM `metal-force-400307.agent.purchase_orders` LIMIT 1"},
	}

	results := make(map[string]interface{})

	for _, test := range tests {
		fmt.Printf("Testing: %s\n", test.name)
		query := bqClient.Query(test.query)
		
		it, err := query.Read(ctx)
		if err != nil {
			fmt.Printf("Test '%s' failed: %v\n", test.name, err)
			results[test.name] = map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			}
			continue
		}

		// Try to read first row
		var row map[string]interface{}
		err = it.Next(&row)
		if err != nil {
			fmt.Printf("Test '%s' result error: %v\n", test.name, err)
			results[test.name] = map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			}
			continue
		}

		fmt.Printf("Test '%s' successful: %v\n", test.name, row)
		results[test.name] = map[string]interface{}{
			"success": true,
			"result":  row,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "BigQuery tests completed",
		"results": results,
	})
}

func getPurchaseOrders(c *gin.Context) {
	fmt.Println("=== getPurchaseOrders endpoint called ===")
	ctx := context.Background()

	queryString := `
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
	`
	
	fmt.Printf("Executing BigQuery query: %s\n", queryString)
	query := bqClient.Query(queryString)

	it, err := query.Read(ctx)
	if err != nil {
		fmt.Printf("BigQuery error details: %v\n", err)
		fmt.Printf("Error type: %T\n", err)
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
			if err.Error() != "iterator done" {
				fmt.Printf("Error reading BigQuery results: %v\n", err)
			}
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

	fmt.Printf("Processed %d items from BigQuery\n", itemCount)
	fmt.Printf("Found %d unique orders\n", len(orderMap))

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

	fmt.Printf("Final response contains %d orders\n", len(response))
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
