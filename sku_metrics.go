package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
)

func getSkuMetrics(c *gin.Context) {
	fmt.Println("SKU metrics requested")
	ctx := context.Background()

	query := bqClient.Query(`
		SELECT 
			sku,
			name,
			category,
			cluster,
			gender,
			imageUrl,
			lead_time,
			purchase_price,
			class,
			size,
			available_count,
			purchased_count,
			sold_last_24_months,
			open_orders_quantity,
			has_half_sizes,
			is_mto,
			season,
			product_id,
			sold_january,
			sold_february,
			sold_march,
			sold_april,
			sold_may,
			sold_june,
			sold_july,
			sold_august,
			sold_september,
			sold_october,
			sold_november,
			sold_december
		FROM metal-force-400307.agent.sku_sizes_metrics
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
			fmt.Printf("Iterator error or done: %v\n", err)
			break
		}
		rowCount++
		fmt.Printf("=== RAW ROW %d ===\n", rowCount)
		fmt.Printf("Values: %v\n", values)
		
		// Convert to map using schema
		row := make(map[string]interface{})
		schema := it.Schema
		for i, field := range schema {
			if i < len(values) {
				row[field.Name] = values[i]
				fmt.Printf("  %s: %v (type: %T)\n", field.Name, values[i], values[i])
			}
		}
		fmt.Printf("=== END ROW %d ===\n", rowCount)
		results = append(results, row)
	}

	fmt.Printf("Returning %d rows\n", len(results))
	c.Header("Cache-Control", "private, max-age=300")
	c.JSON(http.StatusOK, results)
} 