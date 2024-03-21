-- customer_name, customer_nation_name, customer_region_name,
-- supplier_name, supplier_nation_name, supplier_region_name,
-- p_brand as product_brand,
-- p_size as product_size,
-- part_name as product_name
SELECT
  "l_shipmode" as ship_mode,
  "l_linestatus" as line_status,
  "l_returnflag" as return_flag
FROM tpch_1D0g."lineitem"
