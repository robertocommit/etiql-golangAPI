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

	// Configure CORS and origin-based access control
	if os.Getenv("ENV") == "production" {
		// Production: restrict access to specific domains
		allowedOrigins := []string{
			"https://etiql-agent.milhos.tech",
			"https://etiql-checkout-7f00ab87268f.herokuapp.com",
			"https://etiql-checkout-staging-a62191cd7dc2.herokuapp.com",
		}
		
		// Origin-based access control middleware
		router.Use(func(c *gin.Context) {
			origin := c.GetHeader("Origin")
			referer := c.GetHeader("Referer")
			
			// Allow if no origin/referer (for server-to-server calls from allowed domains)
			if origin == "" && referer == "" {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
					"error": "Access denied: Origin required",
				})
				return
			}
			
			// Check origin first
			allowed := false
			if origin != "" {
				for _, allowedOrigin := range allowedOrigins {
					if origin == allowedOrigin {
						allowed = true
						break
					}
				}
			}
			
			// If origin not allowed, check referer
			if !allowed && referer != "" {
				for _, allowedOrigin := range allowedOrigins {
					if len(referer) >= len(allowedOrigin) && referer[:len(allowedOrigin)] == allowedOrigin {
						allowed = true
						break
					}
				}
			}
			
			if !allowed {
				fmt.Printf("Access denied - Origin: %s, Referer: %s\n", origin, referer)
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
					"error": "Access denied: Invalid origin",
					"origin": origin,
					"referer": referer,
				})
				return
			}
			
			c.Next()
		})
		
		// CORS configuration
		config := cors.Config{
			AllowOrigins: allowedOrigins,
			AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With"},
			MaxAge:       12 * time.Hour,
		}
		router.Use(cors.New(config))
		fmt.Println("Production mode: Access restricted to allowed origins only")
	} else {
		// Development: allow all origins
		router.Use(cors.Default())
		fmt.Println("Development mode: All origins allowed")
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


