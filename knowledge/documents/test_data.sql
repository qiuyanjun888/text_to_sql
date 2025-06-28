-- PostgreSQL Test Data Generation
-- Target: Automotive Company Data Warehouse
-- Version: 1.0

-- To ensure idempotency, we'll clear the tables first.
-- The order is important to avoid foreign key violations if they were enforced.
TRUNCATE TABLE fact_after_sales_service, fact_sales, fact_production_log, fact_procurement_orders, fact_parts_inventory, fact_vehicle_inventory RESTART IDENTITY CASCADE;
TRUNCATE TABLE bridge_project_employees, bridge_model_technologies RESTART IDENTITY CASCADE;
TRUNCATE TABLE dim_customers, dim_dealerships, dim_employees, dim_departments, dim_vehicles, dim_factories, dim_suppliers, dim_parts, dim_research_projects, dim_technologies, dim_car_models, dim_date RESTART IDENTITY CASCADE;

-- =================================================================
-- Step 1: Populate Dimension Tables (Independent Dims)
-- =================================================================

-- dim_date: Populate with dates for the last few years
INSERT INTO dim_date (date_id, full_date, year, quarter, month, day, day_of_week, week_of_year, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    EXTRACT(ISODOW FROM d), -- Monday=1, Sunday=7
    EXTRACT(WEEK FROM d),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2021-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) d;

-- dim_departments
INSERT INTO dim_departments (department_id, department_name, description) VALUES
(1, '研发部', '负责新车型和新技术的研发'),
(2, '生产部', '负责车辆的生产和组装'),
(3, '采购部', '负责零部件的采购和供应商管理'),
(4, '销售部', '负责汽车的销售和市场推广'),
(5, '售后服务部', '负责车辆的维修和保养'),
(6, '人力资源部', '负责公司的人员招聘和管理'),
(7, '管理层', '公司高级管理人员');

-- dim_car_models
INSERT INTO dim_car_models (model_id, model_name, model_code, category, base_price, launch_year) VALUES
(1, '星辰-E1', 'XC-E1', '纯电SUV', 280000.00, 2022),
(2, '星辰-S1', 'XC-S1', '纯电轿车', 350000.00, 2023),
(3, '光速-P1', 'GS-P1', '混动轿跑', 420000.00, 2023),
(4, '远征-T1', 'YZ-T1', '混动SUV', 310000.00, 2024);

-- dim_technologies
INSERT INTO dim_technologies (technology_id, technology_name, field) VALUES
(1, '高密度磷酸铁锂电池组', '电池'),
(2, '智能座舱系统 OS 3.0', '智能座舱'),
(3, 'L3级自动驾驶辅助', '自动驾驶'),
(4, '混合动力引擎 Gen2', '动力系统');

-- dim_suppliers
INSERT INTO dim_suppliers (supplier_id, supplier_name, country, rating) VALUES
(1, '宁德时代', '中国', 5),
(2, '博世', '德国', 5),
(3, '福耀玻璃', '中国', 4),
(4, '大陆集团', '德国', 4);

-- dim_parts
INSERT INTO dim_parts (part_id, part_name, part_number, category) VALUES
(1, '电池包总成', 'BAT-LFP-001', '电子'),
(2, '中央控制单元', 'CCU-003', '电子'),
(3, '前挡风玻璃', 'GLS-FR-002', '车身'),
(4, '制动卡钳', 'BRK-CAL-004', '底盘');

-- dim_factories
INSERT INTO dim_factories (factory_id, factory_name, city, production_capacity_per_day) VALUES
(1, '上海超级工厂', '上海', 50),
(2, '广州智能工厂', '广州', 80);

-- dim_dealerships (3 dealerships as requested)
INSERT INTO dim_dealerships (dealership_id, dealership_name, city, manager_id, dealership_level) VALUES
(1, '上海浦东体验中心', '上海', 1, 1),
(2, '北京朝阳旗舰店', '北京', 2, 1),
(3, '广州天河销售中心', '广州', 3, 1);

-- =================================================================
-- Step 2: Populate Dependent Dimension Tables (e.g., Employees)
-- =================================================================

INSERT INTO dim_employees (employee_id, first_name, last_name, job_title, department_id, manager_id, dealership_id) VALUES
-- Managers
(1, '张', '伟', '经销商经理', 7, NULL, 1),
(2, '李', '娜', '经销商经理', 7, NULL, 2),
(3, '王', '强', '经销商经理', 7, NULL, 3),
(4, '赵', '敏', '研发总监', 1, NULL, NULL),
(5, '刘', '洋', '生产总监', 2, NULL, NULL),
-- Other Staff
(10, '周', '平', '项目经理', 1, 4, NULL),
(11, '吴', '磊', '质检员', 2, 5, NULL),
(12, '孙', '静', '技术员', 5, NULL, 1),
-- Salespeople for Dealership 1 (5 people)
(21, '陈', '浩', '高级销售顾问', 4, 1, 1), -- Top performer
(22, '朱', '芳', '销售顾问', 4, 1, 1),
(23, '林', '超', '销售顾问', 4, 1, 1),
(24, '胡', '月', '销售顾问', 4, 1, 1),
(25, '高', '飞', '初级销售顾问', 4, 1, 1), -- Bottom performer
-- Salespeople for Dealership 2 (5 people)
(26, '黄', '鑫', '高级销售顾问', 4, 2, 2), -- Top performer
(27, '马', '琳', '销售顾问', 4, 2, 2),
(28, '罗', '杰', '销售顾问', 4, 2, 2),
(29, '宋', '阳', '销售顾问', 4, 2, 2),
(30, '谢', '婷', '初级销售顾问', 4, 2, 2), -- Bottom performer
-- Salespeople for Dealership 3 (5 people)
(31, '郑', '秀', '高级销售顾问', 4, 3, 3), -- Top performer
(32, '邓', '刚', '销售顾问', 4, 3, 3),
(33, '韩', '雪', '销售顾问', 4, 3, 3),
(34, '唐', '明', '销售顾问', 4, 3, 3),
(35, '曹', '勇', '初级销售顾问', 4, 3, 3); -- Bottom performer

-- =================================================================
-- Step 3: Populate Bridge and Other Dimensions
-- =================================================================

-- bridge_model_technologies
INSERT INTO bridge_model_technologies (model_id, technology_id) VALUES
(1, 1), (1, 2), (1, 3),
(2, 1), (2, 2), (2, 3),
(3, 2), (3, 4),
(4, 2), (4, 4);

-- dim_research_projects
INSERT INTO dim_research_projects (project_id, project_name, start_date_id, project_manager_id) VALUES
(1, '下一代电池技术预研', 20220115, 10);

-- bridge_project_employees
INSERT INTO bridge_project_employees (project_id, employee_id) VALUES
(1, 4), (1, 10);

-- dim_vehicles (Generate 400 vehicles)
INSERT INTO dim_vehicles (vin, model_id, color, trim_level)
SELECT
    'VIN' || LPAD(s::TEXT, 14, '0'),
    (s % 4) + 1,
    CASE WHEN s % 3 = 0 THEN '黑色' WHEN s % 3 = 1 THEN '白色' ELSE '银色' END,
    CASE WHEN s % 2 = 0 THEN '豪华版' ELSE '运动版' END
FROM generate_series(1, 400) s;

-- dim_customers (Generate 200 customers)
INSERT INTO dim_customers (customer_id, first_name, last_name, city, first_purchase_date_id)
SELECT
    s,
    '客户姓' || s,
    '名',
    CASE WHEN s % 3 = 0 THEN '上海' WHEN s % 3 = 1 THEN '北京' ELSE '广州' END,
    NULL -- Will be updated after sales data is generated
FROM generate_series(1, 200) s;

-- =================================================================
-- Step 4: Populate Fact Tables
-- =================================================================

-- fact_production_log
INSERT INTO fact_production_log (vin, factory_id, production_date_id, quality_check_passed, inspector_id)
SELECT
    vin,
    (s % 2) + 1,
    (20230101 + s / 2),
    TRUE,
    11
FROM generate_series(1, 400) s, dim_vehicles v WHERE v.vin = 'VIN' || LPAD(s::TEXT, 14, '0');

-- fact_procurement_orders
INSERT INTO fact_procurement_orders (part_id, supplier_id, order_date_id, quantity, unit_price, total_price)
SELECT (s%4)+1, (s%4)+1, 20230301 + s, 100, 1000.0, 100000.0 FROM generate_series(1, 50) s;

-- fact_parts_inventory (Snapshot)
INSERT INTO fact_parts_inventory (snapshot_date_id, part_id, factory_id, quantity_on_hand)
SELECT 20240101, p.part_id, f.factory_id, 500 FROM dim_parts p, dim_factories f;


-- fact_sales: Core logic for sales distribution
-- Dealership 1 (Shanghai): Sells 60 cars (VIN 1-60). Salespeople: 21-25
--   - SP 21 (Top): 18 cars (VIN 1-18)
--   - SP 22: 13 cars (VIN 19-31)
--   - SP 23: 11 cars (VIN 32-42)
--   - SP 24: 10 cars (VIN 43-52)
--   - SP 25 (Bottom): 8 cars (VIN 53-60)
INSERT INTO fact_sales (vin, customer_id, dealership_id, salesperson_id, sale_date_id, list_price, final_price)
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 1 + (s % 50), 1, 21, 20231001 + s, 300000, 295000 FROM generate_series(1, 18) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 1 + (s % 50), 1, 22, 20231001 + s, 300000, 295000 FROM generate_series(19, 31) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 1 + (s % 50), 1, 23, 20231001 + s, 300000, 295000 FROM generate_series(32, 42) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 1 + (s % 50), 1, 24, 20231001 + s, 300000, 295000 FROM generate_series(43, 52) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 1 + (s % 50), 1, 25, 20231001 + s, 300000, 295000 FROM generate_series(53, 60) s;

-- Dealership 2 (Beijing): Sells 80 cars (VIN 61-140). Salespeople: 26-30
--   - SP 26 (Top): 25 cars (VIN 61-85)
--   - SP 27: 18 cars (VIN 86-103)
--   - SP 28: 15 cars (VIN 104-118)
--   - SP 29: 12 cars (VIN 119-130)
--   - SP 30 (Bottom): 10 cars (VIN 131-140)
INSERT INTO fact_sales (vin, customer_id, dealership_id, salesperson_id, sale_date_id, list_price, final_price)
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 51 + (s % 50), 2, 26, 20231101 + s, 380000, 375000 FROM generate_series(61, 85) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 51 + (s % 50), 2, 27, 20231101 + s, 380000, 375000 FROM generate_series(86, 103) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 51 + (s % 50), 2, 28, 20231101 + s, 380000, 375000 FROM generate_series(104, 118) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 51 + (s % 50), 2, 29, 20231101 + s, 380000, 375000 FROM generate_series(119, 130) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 51 + (s % 50), 2, 30, 20231101 + s, 380000, 375000 FROM generate_series(131, 140) s;

-- Dealership 3 (Guangzhou): Sells 75 cars (VIN 141-215). Salespeople: 31-35
--   - SP 31 (Top): 22 cars (VIN 141-162)
--   - SP 32: 17 cars (VIN 163-179)
--   - SP 33: 15 cars (VIN 180-194)
--   - SP 34: 12 cars (VIN 195-206)
--   - SP 35 (Bottom): 9 cars (VIN 207-215)
INSERT INTO fact_sales (vin, customer_id, dealership_id, salesperson_id, sale_date_id, list_price, final_price)
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 101 + (s % 50), 3, 31, 20231201 + s, 420000, 415000 FROM generate_series(141, 162) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 101 + (s % 50), 3, 32, 20231201 + s, 420000, 415000 FROM generate_series(163, 179) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 101 + (s % 50), 3, 33, 20231201 + s, 420000, 415000 FROM generate_series(180, 194) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 101 + (s % 50), 3, 34, 20231201 + s, 420000, 415000 FROM generate_series(195, 206) s
UNION ALL
SELECT 'VIN' || LPAD(s::TEXT, 14, '0'), 101 + (s % 50), 3, 35, 20231201 + s, 420000, 415000 FROM generate_series(207, 215) s;

-- fact_vehicle_inventory: Populate with unsold cars (VIN 216-400)
INSERT INTO fact_vehicle_inventory (snapshot_date_id, vin, dealership_id, age_in_days, status)
SELECT
    20240115,
    v.vin,
    (s % 3) + 1,
    (20240115 - pl.production_date_id) / 10000 * 365, -- Simplified age calc
    'In Stock'
FROM generate_series(216, 400) s
JOIN dim_vehicles v ON v.vin = 'VIN' || LPAD(s::TEXT, 14, '0')
JOIN fact_production_log pl ON pl.vin = v.vin;

-- fact_after_sales_service
INSERT INTO fact_after_sales_service (vin, customer_id, service_center_id, service_date_id, total_cost)
SELECT
    s.vin,
    s.customer_id,
    s.dealership_id, -- Assume service center is same as dealership for simplicity
    s.sale_date_id + 100, -- Service happens ~100 days after sale
    500.00
FROM fact_sales s WHERE s.sale_id % 10 = 1; -- Some customers come back for service

-- =================================================================
-- Step 5: Update Dimensions Based on Fact Data (e.g., VIP Level)
-- =================================================================

-- Update customer's first purchase date
UPDATE dim_customers
SET first_purchase_date_id = first_sale.min_sale_date
FROM (
    SELECT customer_id, MIN(sale_date_id) as min_sale_date
    FROM fact_sales
    GROUP BY customer_id
) AS first_sale
WHERE dim_customers.customer_id = first_sale.customer_id;

-- Update customer VIP levels based on number of cars purchased
WITH customer_purchases AS (
    SELECT customer_id, COUNT(vin) as num_cars
    FROM fact_sales
    GROUP BY customer_id
)
UPDATE dim_customers
SET vip_level =
    CASE
        WHEN cp.num_cars >= 10 THEN 5
        WHEN cp.num_cars >= 5 THEN 4
        WHEN cp.num_cars >= 3 THEN 3
        WHEN cp.num_cars >= 2 THEN 2
        ELSE 1
    END
FROM customer_purchases cp
WHERE dim_customers.customer_id = cp.customer_id;

-- Make sure even customers with 1 purchase have a VIP level
UPDATE dim_customers
SET vip_level = 1
WHERE vip_level IS NULL AND first_purchase_date_id IS NOT NULL; 