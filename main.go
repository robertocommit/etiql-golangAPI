// main.go
package main

import (
	"context"
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
			// Production path
			serviceAccountPath = "/shared/volumes/a21e22/service_account_dbt.json"
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
			// Just log the first 200 characters to see the structure (avoid logging sensitive data)
			if len(fileContent) > 200 {
				fmt.Printf("Service account file preview: %s...\n", string(fileContent[:200]))
			} else {
				fmt.Printf("Service account file content length: %d bytes\n", len(fileContent))
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
