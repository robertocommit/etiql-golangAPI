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

	bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", option.WithCredentialsFile(serviceAccountPath))

	if err != nil {
		panic(fmt.Sprintf("Failed to create BigQuery client: %v", err))
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

	it, err := query.Read(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query BigQuery"})
		return
	}

	// Group items by order ID
	orderMap := make(map[int64]map[string][]BigQueryOrderItem)

	for {
		var item BigQueryOrderItem
		err := it.Next(&item)
		if err != nil {
			break
		}

		if orderMap[item.ID] == nil {
			orderMap[item.ID] = make(map[string][]BigQueryOrderItem)
		}
		if orderMap[item.ID][item.DeliveryDate] == nil {
			orderMap[item.ID][item.DeliveryDate] = []BigQueryOrderItem{}
		}
		orderMap[item.ID][item.DeliveryDate] = append(orderMap[item.ID][item.DeliveryDate], item)
	}

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
