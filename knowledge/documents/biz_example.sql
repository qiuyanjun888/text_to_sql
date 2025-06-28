-- =================================================================
-- 业务场景SQL查询示例 (Business Scenarios SQL Examples)
-- 数据库: PostgreSQL
-- =================================================================

-- -----------------------------------------------------
-- 场景1: 销售业绩查询 (Sales Performance)
-- -----------------------------------------------------

-- 查询1.1: 各经销商销售业绩排名 (按销售额)
-- 业务说明: 按总销售额对所有经销商进行排名，用于评估各经销商的整体贡献。
SELECT
    d.dealership_name,
    COUNT(s.sale_id) AS total_sales_count,
    SUM(s.final_price) AS total_sales_amount
FROM fact_sales s
JOIN dim_dealerships d ON s.dealership_id = d.dealership_id
GROUP BY d.dealership_name
ORDER BY total_sales_amount DESC;

-- 查询1.2: 特定经销商内部销售员业绩排名
-- 业务说明: 查看"上海浦东体验中心"内部销售顾问的业绩，按销售额和销量排名。
SELECT
    e.first_name || e.last_name AS salesperson_name,
    COUNT(s.sale_id) AS sales_count,
    SUM(s.final_price) AS total_amount
FROM fact_sales s
JOIN dim_employees e ON s.salesperson_id = e.employee_id
JOIN dim_dealerships d ON s.dealership_id = d.dealership_id
WHERE d.dealership_name = '上海浦东体验中心'
GROUP BY salesperson_name
ORDER BY total_amount DESC;

-- 查询1.3: 按月份统计公司总销售额
-- 业务说明: 查看公司在2023年每个月的销售总额，分析销售趋势。
SELECT
    d.year,
    d.month,
    SUM(s.final_price) AS monthly_sales_amount
FROM fact_sales s
JOIN dim_date d ON s.sale_date_id = d.date_id
WHERE d.year = 2023
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- -----------------------------------------------------
-- 场景2: 库存查询 (Inventory Inquiry)
-- -----------------------------------------------------

-- 查询2.1: 各经销商的车型库存概览
-- 业务说明: 查询每个经销商处，不同车型的库存数量。
SELECT
    dd.dealership_name,
    dm.model_name,
    dm.category,
    COUNT(fvi.vin) AS stock_quantity
FROM fact_vehicle_inventory fvi
JOIN dim_dealerships dd ON fvi.dealership_id = dd.dealership_id
JOIN dim_vehicles dv ON fvi.vin = dv.vin
JOIN dim_car_models dm ON dv.model_id = dm.model_id
GROUP BY dd.dealership_name, dm.model_name, dm.category
ORDER BY dd.dealership_name, stock_quantity DESC;

-- 查询2.2: 查询指定经销商处库龄超过90天的车辆
-- 业务说明: 识别"北京朝阳旗舰店"的长老库存车辆，以便进行促销或调拨。
SELECT
    fvi.vin,
    dm.model_name,
    dv.color,
    fvi.age_in_days
FROM fact_vehicle_inventory fvi
JOIN dim_dealerships dd ON fvi.dealership_id = dd.dealership_id
JOIN dim_vehicles dv ON fvi.vin = dv.vin
JOIN dim_car_models dm ON dv.model_id = dm.model_id
WHERE dd.dealership_name = '北京朝阳旗舰店' AND fvi.age_in_days > 90
ORDER BY fvi.age_in_days DESC;


-- -----------------------------------------------------
-- 场景3: 客户价值分析 (Customer Value Analysis)
-- -----------------------------------------------------

-- 查询3.1: 按VIP等级统计客户数量和总消费额
-- 业务说明: 分析不同VIP等级的客户数量及其贡献的总销售额，识别高价值客户群体。
SELECT
    dc.vip_level,
    COUNT(DISTINCT dc.customer_id) AS number_of_customers,
    SUM(fs.final_price) AS total_spending
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
GROUP BY dc.vip_level
ORDER BY dc.vip_level DESC;

-- 查询3.2: 找出消费总额最高的前10名客户 (Top 10 Customers)
-- 业务说明: 列出最有价值的客户，用于精准营销或提供专属服务。
SELECT
    c.customer_id,
    c.first_name || c.last_name AS customer_name,
    c.vip_level,
    SUM(s.final_price) AS total_spent,
    COUNT(s.sale_id) AS total_purchases
FROM fact_sales s
JOIN dim_customers c ON s.customer_id = c.customer_id
GROUP BY c.customer_id, customer_name, c.vip_level
ORDER BY total_spent DESC
LIMIT 10;

-- -----------------------------------------------------
-- 场景4: 员工业绩与团队分析
-- -----------------------------------------------------

-- 查询4.1: 查询每个经理所管理团队的总业绩
-- 业务说明: 通过员工的上下级关系，计算每位经理所带领团队的总销售额。
WITH RECURSIVE team_hierarchy AS (
    -- Anchor member: a manager is part of their own "team" scope
    SELECT employee_id, manager_id, employee_id as top_level_manager_id
    FROM dim_employees
    WHERE manager_id IS NOT NULL

    UNION ALL

    -- Recursive member: find all subordinates
    SELECT e.employee_id, e.manager_id, th.top_level_manager_id
    FROM dim_employees e
    JOIN team_hierarchy th ON e.manager_id = th.employee_id
)
SELECT
    manager.first_name || manager.last_name as manager_name,
    SUM(fs.final_price) as team_sales_amount,
    COUNT(fs.sale_id) as team_sales_count
FROM fact_sales fs
JOIN (
    SELECT DISTINCT employee_id, top_level_manager_id
    FROM team_hierarchy
    -- This CTE finds all employees under each top-level manager
) team_members ON fs.salesperson_id = team_members.employee_id
JOIN dim_employees manager ON team_members.top_level_manager_id = manager.employee_id
GROUP BY manager_name
ORDER BY team_sales_amount DESC;

-- -----------------------------------------------------
-- 场景5: 综合分析 (Comprehensive Analysis)
-- -----------------------------------------------------

-- 查询5.1: 2023年第四季度各车型在各城市的销售情况
-- 业务说明: 综合分析特定时间段内，不同车型在主要市场的表现，为市场策略提供数据支持。
SELECT
    dd.city,
    dcm.model_name,
    COUNT(fs.sale_id) AS sales_volume,
    SUM(fs.final_price) AS sales_revenue
FROM fact_sales fs
JOIN dim_date dd_sale ON fs.sale_date_id = dd_sale.date_id
JOIN dim_dealerships dd ON fs.dealership_id = dd.dealership_id
JOIN dim_vehicles dv ON fs.vin = dv.vin
JOIN dim_car_models dcm ON dv.model_id = dcm.model_id
WHERE dd_sale.year = 2023 AND dd_sale.quarter = 4
GROUP BY dd.city, dcm.model_name
ORDER BY dd.city, sales_revenue DESC; 