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
		fmt.Println("FORCE_ADC=true, using Application Default Credentials")
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307",
			option.WithScopes("https://www.googleapis.com/auth/bigquery"))
		if err != nil {
			panic(fmt.Sprintf("Failed to create BigQuery client with ADC: %v", err))
		}
		fmt.Println("BigQuery client created successfully using forced ADC")
	} else if credOption, err := createCredentialsFromEnv(); err == nil {
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", credOption)
		if err != nil {
			panic(fmt.Sprintf("Failed to create BigQuery client with env credentials: %v", err))
		}
	} else {
		bqClient, err = bigquery.NewClient(ctx, "metal-force-400307", 
			option.WithCredentialsFile(serviceAccountPath))
	}

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
		if err := router.Run(fmt.Sprintf(":%s", os.Getenv("PORT"))); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	} else {
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
