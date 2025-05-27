package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"google.golang.org/api/option"
)

var (
	bqClient *bigquery.Client
)

func loadEnvFile() {
	file, err := os.Open(".env")
	if err != nil {
		fmt.Println("No .env file found, using system environment variables")
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Only set if not already set in environment
			if os.Getenv(key) == "" {
				os.Setenv(key, value)
				fmt.Printf("Loaded from .env: %s=%s\n", key, value)
			}
		}
	}
}

func main() {
	// Load .env file if it exists
	loadEnvFile()
	
	env := os.Getenv("ENV")
	port := os.Getenv("PORT")
	fmt.Printf("Starting server - Environment: '%s'\n", env)
	fmt.Printf("Port: '%s'\n", port)
	fmt.Printf("Environment check: ENV=='production' = %t\n", env == "production")

	// Initialize BigQuery client
	ctx := context.Background()
	var err error

	serviceAccountPath := "./golang-api-bigquery.json"

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

	// Configure CORS
	if env == "production" {
		// Production: restrict access to specific hardcoded domains
		allowedOrigins := []string{
			"https://etiql-agent.milhos.tech",
			"https://etiql-checkout-7f00ab87268f.herokuapp.com",
			"https://etiql-checkout-staging-a62191cd7dc2.herokuapp.com",
		}
		
		router.Use(cors.New(cors.Config{
			AllowOriginFunc: func(origin string) bool {
				// Check exact matches
				for _, allowedOrigin := range allowedOrigins {
					if allowedOrigin == origin {
						fmt.Printf("CORS: Allowing origin: %s\n", origin)
						return true
					}
				}
				fmt.Printf("CORS: Blocking origin: %s\n", origin)
				return false
			},
			AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"},
			AllowHeaders: []string{
				"Accept",
				"Accept-Language",
				"Content-Type",
				"Content-Length",
				"Accept-Encoding",
				"X-CSRF-Token",
				"Authorization",
				"Cache-Control",
				"X-Requested-With",
				"Origin",
			},
			ExposeHeaders:    []string{"Content-Length"},
			AllowCredentials: true,
			MaxAge:           12 * time.Hour,
		}))
		fmt.Println("Production mode: CORS restricted to allowed origins only")
	} else {
		// Development: allow all origins
		router.Use(cors.New(cors.Config{
			AllowAllOrigins: true,
			AllowMethods:    []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"},
			AllowHeaders: []string{
				"Accept",
				"Accept-Language",
				"Content-Type",
				"Content-Length",
				"Accept-Encoding",
				"X-CSRF-Token",
				"Authorization",
				"Cache-Control",
				"X-Requested-With",
				"Origin",
			},
			ExposeHeaders:    []string{"Content-Length"},
			AllowCredentials: true,
			MaxAge:           12 * time.Hour,
		}))
		fmt.Println("Development mode: All origins allowed")
	}

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Server is running"})
	})

	router.GET("/purchase-orders", getPurchaseOrders)
	router.GET("/all-purchase-orders", getAllPurchaseOrders)
	router.GET("/sku-metrics", getSkuMetrics)

	if env == "production" {
		fmt.Printf("Starting server in production mode on port %s\n", port)
		if err := router.Run(fmt.Sprintf(":%s", port)); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	} else {
		if port == "" {
			port = "8011"
		}
		fmt.Printf("Starting server in development mode on port %s\n", port)
		if err := router.Run(fmt.Sprintf(":%s", port)); err != nil {
			panic(fmt.Sprintf("Failed to start server: %v", err))
		}
	}
}


