package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"google.golang.org/api/iterator"
)

func getAllPurchaseOrders(c *gin.Context) {
	fmt.Println("All purchase orders requested")
	ctx := context.Background()

	query := bqClient.Query(`
		WITH filtered_items AS (
			SELECT * EXCEPT(items),
				ARRAY(
					SELECT AS STRUCT * 
					FROM UNNEST(items) 
					WHERE product_id != 0
				) as items
			FROM metal-force-400307.agent.purchase_orders
		)
		SELECT * FROM filtered_items
		WHERE ARRAY_LENGTH(items) > 0
	`)

	it, err := query.Read(ctx)
	if err != nil {
		fmt.Printf("BigQuery error: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to query BigQuery",
			"details": err.Error(),
		})
		return
	}

	// Use map[string]bigquery.Value to preserve raw BigQuery structure
	var results []map[string]bigquery.Value
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("Error reading row: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to read BigQuery results",
				"details": err.Error(),
			})
			return
		}
		results = append(results, row)
	}

	fmt.Printf("Returning %d purchase orders in raw BigQuery format\n", len(results))
	c.Header("Cache-Control", "private, max-age=300")
	c.JSON(http.StatusOK, results)
} 