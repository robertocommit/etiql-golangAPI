package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

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

	// Bearer token authentication middleware
	router.Use(func(c *gin.Context) {
		// Skip auth for root endpoint
		if c.Request.URL.Path == "/" {
			c.Next()
			return
		}

		authHeader := c.GetHeader("Authorization")
		userAgent := c.GetHeader("User-Agent")
		origin := c.GetHeader("Origin")
		referer := c.GetHeader("Referer")
		
		fmt.Printf("=== AUTH DEBUG ===\n")
		fmt.Printf("Method: %s\n", c.Request.Method)
		fmt.Printf("Path: %s\n", c.Request.URL.Path)
		fmt.Printf("Authorization Header: '%s'\n", authHeader)
		fmt.Printf("User-Agent: '%s'\n", userAgent)
		fmt.Printf("Origin: '%s'\n", origin)
		fmt.Printf("Referer: '%s'\n", referer)
		fmt.Printf("Remote Address: %s\n", c.ClientIP())
		fmt.Printf("==================\n")

		// Check for Bearer token
		if authHeader == "" {
			fmt.Printf("AUTH: No Authorization header provided\n")
			c.AbortWithStatusJSON(401, gin.H{
				"error": "Unauthorized",
				"message": "Bearer token required. Use: Authorization: Bearer YOUR_TOKEN",
			})
			return
		}

		// Extract token from "Bearer TOKEN"
		if len(authHeader) < 7 || authHeader[:7] != "Bearer " {
			fmt.Printf("AUTH: Invalid Authorization format: %s\n", authHeader)
			c.AbortWithStatusJSON(401, gin.H{
				"error": "Unauthorized", 
				"message": "Invalid Authorization format. Use: Authorization: Bearer YOUR_TOKEN",
			})
			return
		}

		token := authHeader[7:] // Remove "Bearer " prefix
		validToken := os.Getenv("API_TOKEN")
		
		if validToken == "" {
			fmt.Printf("AUTH: No API_TOKEN environment variable set\n")
			c.AbortWithStatusJSON(500, gin.H{
				"error": "Server configuration error",
				"message": "API token not configured",
			})
			return
		}

		if token != validToken {
			fmt.Printf("AUTH: Invalid token provided: %s\n", token)
			c.AbortWithStatusJSON(401, gin.H{
				"error": "Unauthorized",
				"message": "Invalid bearer token",
			})
			return
		}

		fmt.Printf("AUTH: Valid token provided, allowing access\n")
		c.Next()
	})

	// Simple CORS for development
	router.Use(cors.Default())
	fmt.Println("Authentication: Bearer token required for all endpoints except /")
	apiToken := os.Getenv("API_TOKEN")
	if apiToken != "" {
		fmt.Printf("API_TOKEN configured: %s\n", apiToken)
	} else {
		fmt.Println("WARNING: API_TOKEN environment variable not set!")
	}

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Server is running"})
	})

	router.GET("/purchase-orders", getPurchaseOrders)
	router.GET("/all-purchase-orders", getAllPurchaseOrders)
	router.GET("/sku-metrics", getSkuMetrics)
	router.GET("/sku-metrics/:sku_id", getSkuMetricsSingle)

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


