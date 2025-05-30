package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
)

func getSkuMetricsSingle(c *gin.Context) {
	skuId := c.Param("sku_id")
	if skuId == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "SKU ID is required",
		})
		return
	}

	fmt.Printf("SKU metrics requested for: %s\n", skuId)
	ctx := context.Background()

	// Modified query to work with a single SKU without etiql_agent_seed
	query := bqClient.Query(`
		WITH latest_inventory_date AS (
		  SELECT MAX(date) AS max_date
		  FROM metal-force-400307.staging.stg_xentral__inventory
		  WHERE warehouse IS NOT NULL
		),
		inventory_metrics AS (
		  SELECT
		      v.base_sku AS sku,
		      v.size,
		      SUM(i.quantity) AS available_count
		  FROM metal-force-400307.staging.stg_shopify__products_variant v
		  LEFT JOIN metal-force-400307.staging.stg_xentral__products p ON v.sku = p.sku
		  LEFT JOIN metal-force-400307.staging.stg_xentral__inventory i ON p.id = i.product_id
		  CROSS JOIN latest_inventory_date lid
		  WHERE i.warehouse IS NOT NULL
		  AND i.date = lid.max_date
		  AND v.base_sku = @sku_id
		  GROUP BY 1, 2
		),
		purchased_items AS(
		  SELECT
		      pod.base_sku AS sku,
		      pod.size,
		      SUM(pod.quantity) AS purchased_count
		  FROM metal-force-400307.staging.stg_xentral__purchase_order_details pod
		  WHERE CAST(pod.confirmed_delivery_date AS DATE) >= CURRENT_DATE()
		  AND pod.base_sku = @sku_id
		  GROUP BY 1, 2
		),
		sold_items_total AS (
		  SELECT
		    v.base_sku AS sku,
		    SUBSTRING(v.sku, 10) AS size,
		    SUM(o.item_quantity) AS total_sold
		  FROM metal-force-400307.staging.stg_shopify__orders_items o
		  LEFT JOIN metal-force-400307.staging.stg_shopify__products_variant v ON o.variant_id = v.id
		  WHERE EXTRACT(DATE FROM o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
		  AND v.base_sku = @sku_id
		  GROUP BY 1, 2
		),
		sold_items_monthly AS (
		  SELECT
		    v.base_sku AS sku,
		    SUBSTRING(v.sku, 10) AS size,
		    FORMAT_DATE('%Y%m', o.created_at) AS year_month,
		    FORMAT_DATE('%B', o.created_at) AS month_name,
		    EXTRACT(MONTH FROM o.created_at) AS month_number,
		    EXTRACT(YEAR FROM o.created_at) AS year,
		    SUM(o.item_quantity) AS monthly_sold
		  FROM metal-force-400307.staging.stg_shopify__orders_items o
		  LEFT JOIN metal-force-400307.staging.stg_shopify__products_variant v ON o.variant_id = v.id
		  WHERE EXTRACT(DATE FROM o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
		  AND v.base_sku = @sku_id
		  GROUP BY 1, 2, 3, 4, 5, 6
		),
		sold_items_monthly_pivot AS (
		  SELECT
		    sku,
		    size,
		    MAX(CASE WHEN month_name = 'January' THEN monthly_sold ELSE 0 END) AS sold_january,
		    MAX(CASE WHEN month_name = 'February' THEN monthly_sold ELSE 0 END) AS sold_february,
		    MAX(CASE WHEN month_name = 'March' THEN monthly_sold ELSE 0 END) AS sold_march,
		    MAX(CASE WHEN month_name = 'April' THEN monthly_sold ELSE 0 END) AS sold_april,
		    MAX(CASE WHEN month_name = 'May' THEN monthly_sold ELSE 0 END) AS sold_may,
		    MAX(CASE WHEN month_name = 'June' THEN monthly_sold ELSE 0 END) AS sold_june,
		    MAX(CASE WHEN month_name = 'July' THEN monthly_sold ELSE 0 END) AS sold_july,
		    MAX(CASE WHEN month_name = 'August' THEN monthly_sold ELSE 0 END) AS sold_august,
		    MAX(CASE WHEN month_name = 'September' THEN monthly_sold ELSE 0 END) AS sold_september,
		    MAX(CASE WHEN month_name = 'October' THEN monthly_sold ELSE 0 END) AS sold_october,
		    MAX(CASE WHEN month_name = 'November' THEN monthly_sold ELSE 0 END) AS sold_november,
		    MAX(CASE WHEN month_name = 'December' THEN monthly_sold ELSE 0 END) AS sold_december
		  FROM sold_items_monthly
		  GROUP BY 1, 2
		),
		open_orders AS (
		  SELECT
		    SUBSTRING(o.product_sku, 1, 9) AS base_sku,
		    SUBSTRING(o.product_sku, 10) AS size,
		    COUNT(*) as quantity
		  FROM metal-force-400307.staging.stg_xentral__open_orders o
		  WHERE o.product_sku IS NOT NULL
		  AND o.order_date >= '2024-09-01'
		  AND SUBSTRING(o.product_sku, 1, 9) = @sku_id
		  GROUP BY 1, 2
		),
		all_sizes AS (
		  SELECT DISTINCT sku, size
		  FROM (
		    SELECT sku, size FROM inventory_metrics WHERE size IS NOT NULL
		    UNION ALL
		    SELECT sku, size FROM purchased_items WHERE size IS NOT NULL
		    UNION ALL
		    SELECT sku, size FROM sold_items_total WHERE size IS NOT NULL
		    UNION ALL
		    SELECT base_sku as sku, size FROM open_orders WHERE size IS NOT NULL
		  )
		)
		SELECT DISTINCT
		  a.sku,
		  pr.id as product_id,
		  CASE
		    WHEN a.size = '385' THEN '38.5'
		    WHEN a.size = '395' THEN '39.5'
		    WHEN a.size = '425' THEN '42.5'
		    WHEN a.size = '435' THEN '43.5'
		    ELSE a.size
		  END AS size,
		  COALESCE(i.available_count, 0) as available_count,
		  COALESCE(p.purchased_count, 0) as purchased_count,
		  COALESCE(st.total_sold, 0) as sold_last_24_months,
		  COALESCE(sm.sold_january, 0) as sold_january,
		  COALESCE(sm.sold_february, 0) as sold_february,
		  COALESCE(sm.sold_march, 0) as sold_march,
		  COALESCE(sm.sold_april, 0) as sold_april,
		  COALESCE(sm.sold_may, 0) as sold_may,
		  COALESCE(sm.sold_june, 0) as sold_june,
		  COALESCE(sm.sold_july, 0) as sold_july,
		  COALESCE(sm.sold_august, 0) as sold_august,
		  COALESCE(sm.sold_september, 0) as sold_september,
		  COALESCE(sm.sold_october, 0) as sold_october,
		  COALESCE(sm.sold_november, 0) as sold_november,
		  COALESCE(sm.sold_december, 0) as sold_december,
		  COALESCE(o.quantity, 0) as open_orders_quantity
		FROM all_sizes a
		LEFT JOIN inventory_metrics i ON a.sku = i.sku AND a.size = i.size
		LEFT JOIN purchased_items p ON a.sku = p.sku AND a.size = p.size
		LEFT JOIN sold_items_total st ON a.sku = st.sku AND a.size = st.size
		LEFT JOIN sold_items_monthly_pivot sm ON a.sku = sm.sku AND a.size = sm.size
		LEFT JOIN open_orders o ON a.sku = o.base_sku AND a.size = o.size
		LEFT JOIN metal-force-400307.staging.stg_xentral__products pr ON CONCAT(a.sku, a.size) = pr.sku
		WHERE a.sku = @sku_id
	`)

	// Set the SKU parameter
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "sku_id",
			Value: skuId,
		},
	}

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
			if err.Error() == "no more items in iterator" {
				// Normal end of iteration
				break
			}
			fmt.Printf("Iterator error: %v\n", err)
			break
		}
		rowCount++
		fmt.Printf("=== ROW %d for SKU %s ===\n", rowCount, skuId)
		
		// Convert to map using schema
		row := make(map[string]interface{})
		schema := it.Schema
		for i, field := range schema {
			if i < len(values) {
				row[field.Name] = values[i]
				fmt.Printf("  %s: %v\n", field.Name, values[i])
			}
		}
		results = append(results, row)
	}

	fmt.Printf("Returning %d size records for SKU %s\n", len(results), skuId)
	
	// Return 404 if no data found for this SKU
	if len(results) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "SKU not found",
			"sku":   skuId,
		})
		return
	}

	c.Header("Cache-Control", "private, max-age=300")
	c.JSON(http.StatusOK, results)
}

// Add this route to your main router setup:
// router.GET("/sku-metrics/:sku_id", getSkuMetricsSingle) 