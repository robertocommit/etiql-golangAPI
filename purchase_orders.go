package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
)



func getPurchaseOrders(c *gin.Context) {
	fmt.Println("Purchase orders requested")
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

	fmt.Printf("Returning %d purchase order items\n", len(results))
	c.Header("Cache-Control", "private, max-age=300")
	c.JSON(http.StatusOK, results)
} 