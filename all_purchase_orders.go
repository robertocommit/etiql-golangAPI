package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
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

	var results []map[string]interface{}
	rowCount := 0
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err != nil {
			break
		}
		rowCount++
		
		// Convert to map using schema
		row := make(map[string]interface{})
		schema := it.Schema
		for i, field := range schema {
			if i < len(values) {
				row[field.Name] = values[i]
			}
		}
		results = append(results, row)
	}

	fmt.Printf("Returning %d purchase orders\n", len(results))
	c.Header("Cache-Control", "private, max-age=300")
	c.JSON(http.StatusOK, results)
} 