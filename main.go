package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"google.golang.org/api/option"
)

var (
	bqClient *bigquery.Client
)

func main() {
	fmt.Printf("Starting server - Environment: %s\n", os.Getenv("ENV"))

	// Initialize BigQuery client
	ctx := context.Background()
	var err error

	serviceAccountPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if serviceAccountPath == "" {
		if os.Getenv("ENV") == "production" {
			serviceAccountPath = "/shared/volumes/a21e22/golang-api-bigquery.json"
		} else {
			serviceAccountPath = "./golang-api-bigquery.json"
		}
	}

	fmt.Printf("Using service account: %s\n", serviceAccountPath)

	if _, err := os.Stat(serviceAccountPath); os.IsNotExist(err) {
		panic(fmt.Sprintf("Service account file does not exist at: %s", serviceAccountPath))
	}

	bqClient, err = bigquery.NewClient(ctx, "metal-force-400307",
		option.WithCredentialsFile(serviceAccountPath))

	if err != nil {
		panic(fmt.Sprintf("Failed to create BigQuery client: %v", err))
	}
	fmt.Println("BigQuery client initialized")
	defer bqClient.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// Configure CORS based on environment
	if os.Getenv("ENV") == "production" {
		// Production: restrict to specific domains
		config := cors.Config{
			AllowOrigins: []string{
				"https://etiql-agent.milhos.tech",
				"https://etiql-checkout-7f00ab87268f.herokuapp.com",
				"https://etiql-checkout-staging-a62191cd7dc2.herokuapp.com",
			},
			AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With"},
			MaxAge:       12 * time.Hour,
		}
		router.Use(cors.New(config))
		fmt.Println("CORS configured for production with restricted origins")
	} else {
		// Development: allow all origins
		router.Use(cors.Default())
		fmt.Println("CORS configured for development (all origins allowed)")
	}

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Server is running"})
	})

	router.GET("/purchase-orders", getPurchaseOrders)
	router.GET("/all-purchase-orders", getAllPurchaseOrders)
	router.GET("/sku-metrics", getSkuMetrics)

	if os.Getenv("ENV") == "production" {
		fmt.Printf("Starting server in production mode on port %s\n", os.Getenv("PORT"))
		if err := router.Run(fmt.Sprintf(":%s", os.Getenv("PORT"))); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	} else {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8011"
		}
		fmt.Printf("Starting server in development mode on port %s\n", port)
		if err := router.Run(fmt.Sprintf(":%s", port)); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	}
}


