-- =================================================================
-- 汽车公司数据仓库 - 星型模型设计
-- 涵盖范围: 研发, 采购, 生产, 库存, 销售, 人力资源, 财务
-- 设计原则:
-- 1. 星型模型: 中心是事实表, 周围是维度表。
-- 2. 无物理外键: 表之间的关联通过ID引用实现。
-- =================================================================

-- -----------------------------------------------------
-- 通用维度表 (Shared Dimensions)
-- -----------------------------------------------------

-- 部门维度表
CREATE TABLE dim_departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    description TEXT
);

-- 员工维度表
CREATE TABLE dim_employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    job_title VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    phone_number VARCHAR(20),
    hire_date DATE,
    department_id INT, -- 关联 dim_departments.department_id
    manager_id INT, -- 关联 dim_employees.employee_id (自引用)
    dealership_id INT -- 关联 dim_dealerships.dealership_id (适用于销售或服务中心员工)
);

-- 日期维度表
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY, -- e.g., 20240101
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT, -- 1 for Sunday, 7 for Saturday
    week_of_year INT,
    is_weekend BOOLEAN
);

-- -----------------------------------------------------
-- 研发 (R&D)
-- -----------------------------------------------------

-- 车型维度表
CREATE TABLE dim_car_models (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_code VARCHAR(20) UNIQUE NOT NULL,
    category VARCHAR(50), -- e.g., SUV, Sedan, EV
    base_price DECIMAL(12, 2),
    launch_year INT
);

-- 技术维度表
CREATE TABLE dim_technologies (
    technology_id SERIAL PRIMARY KEY,
    technology_name VARCHAR(150) NOT NULL,
    description TEXT,
    patent_status VARCHAR(50), -- e.g., Patented, Pending
    field VARCHAR(100) -- e.g., Battery, Autonomous Driving, Powertrain
);

-- 研发项目维度表
CREATE TABLE dim_research_projects (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(200) NOT NULL,
    start_date_id INT, -- 关联 dim_date.date_id
    end_date_id INT, -- 关联 dim_date.date_id
    budget DECIMAL(15, 2),
    status VARCHAR(50), -- e.g., Ongoing, Completed, Cancelled
    project_manager_id INT -- 关联 dim_employees.employee_id
);

-- 车型与技术关联表 (Bridge Table)
CREATE TABLE bridge_model_technologies (
    model_id INT NOT NULL, -- 关联 dim_car_models.model_id
    technology_id INT NOT NULL, -- 关联 dim_technologies.technology_id
    PRIMARY KEY (model_id, technology_id)
);

-- 项目与员工关联表 (Bridge Table)
CREATE TABLE bridge_project_employees (
    project_id INT NOT NULL, -- 关联 dim_research_projects.project_id
    employee_id INT NOT NULL, -- 关联 dim_employees.employee_id
    PRIMARY KEY (project_id, employee_id)
);

-- -----------------------------------------------------
-- 采购 (Procurement)
-- -----------------------------------------------------

-- 零部件维度表
CREATE TABLE dim_parts (
    part_id SERIAL PRIMARY KEY,
    part_name VARCHAR(150) NOT NULL,
    part_number VARCHAR(50) UNIQUE NOT NULL,
    category VARCHAR(100), -- e.g., Engine, Electronics, Body
    standard_cost DECIMAL(10, 2)
);

-- 供应商维度表
CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100),
    contact_person VARCHAR(100),
    rating INT -- e.g., 1-5
);

-- 采购订单事实表
CREATE TABLE fact_procurement_orders (
    procurement_order_id SERIAL PRIMARY KEY,
    part_id INT NOT NULL, -- 关联 dim_parts.part_id
    supplier_id INT NOT NULL, -- 关联 dim_suppliers.supplier_id
    order_date_id INT NOT NULL, -- 关联 dim_date.date_id
    expected_delivery_date_id INT, -- 关联 dim_date.date_id
    received_date_id INT, -- 关联 dim_date.date_id
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(15, 2) NOT NULL,
    status VARCHAR(50) -- e.g., Ordered, Shipped, Received
);

-- -----------------------------------------------------
-- 生产与库存 (Production & Inventory)
-- -----------------------------------------------------

-- 工厂维度表
CREATE TABLE dim_factories (
    factory_id SERIAL PRIMARY KEY,
    factory_name VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100),
    production_capacity_per_day INT
);

-- 整车唯一标识 (VIN) 维度表
-- 作为一个退化维度，也可以直接在事实表中包含VIN字符串
CREATE TABLE dim_vehicles (
    vin VARCHAR(17) PRIMARY KEY, -- Vehicle Identification Number
    model_id INT NOT NULL, -- 关联 dim_car_models.model_id
    color VARCHAR(50),
    trim_level VARCHAR(50) -- e.g., Standard, Sport, Luxury
);

-- 生产日志事实表
CREATE TABLE fact_production_log (
    production_log_id SERIAL PRIMARY KEY,
    vin VARCHAR(17) NOT NULL, -- 关联 dim_vehicles.vin
    factory_id INT NOT NULL, -- 关联 dim_factories.factory_id
    production_date_id INT NOT NULL, -- 关联 dim_date.date_id
    assembly_time_minutes INT,
    quality_check_passed BOOLEAN,
    inspector_id INT -- 关联 dim_employees.employee_id
);

-- 整车库存事实表 (每日快照)
CREATE TABLE fact_vehicle_inventory (
    inventory_snapshot_id SERIAL PRIMARY KEY,
    snapshot_date_id INT NOT NULL, -- 关联 dim_date.date_id
    vin VARCHAR(17) NOT NULL, -- 关联 dim_vehicles.vin
    dealership_id INT NOT NULL, -- 关联 dim_dealerships.dealership_id
    age_in_days INT, -- 库存天数
    status VARCHAR(50) -- e.g., 'In Stock', 'Reserved'
);

-- 零部件库存事实表
CREATE TABLE fact_parts_inventory (
    inventory_snapshot_id SERIAL PRIMARY KEY,
    snapshot_date_id INT NOT NULL, -- 关联 dim_date.date_id
    part_id INT NOT NULL, -- 关联 dim_parts.part_id
    factory_id INT NOT NULL, -- 关联 dim_factories.factory_id
    quantity_on_hand INT NOT NULL,
    reorder_level INT
);

-- -----------------------------------------------------
-- 销售与客户 (Sales & Customers)
-- -----------------------------------------------------

-- 经销商维度表
CREATE TABLE dim_dealerships (
    dealership_id SERIAL PRIMARY KEY,
    dealership_name VARCHAR(150) NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100),
    address VARCHAR(255),
    manager_id INT, -- 关联 dim_employees.employee_id
    dealership_level INT, -- 1: 总代理, 2: 二级代理, 3: 三级代理
    parent_dealership_id INT -- 关联 dim_dealerships.dealership_id (自引用，上级代理)
);

-- 客户维度表
CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    first_purchase_date_id INT, -- 关联 dim_date.date_id
    vip_level INT -- 客户VIP等级, e.g., 1-5
);

-- 销售事实表
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    vin VARCHAR(17) NOT NULL, -- 关联 dim_vehicles.vin
    customer_id INT NOT NULL, -- 关联 dim_customers.customer_id
    dealership_id INT NOT NULL, -- 关联 dim_dealerships.dealership_id
    salesperson_id INT NOT NULL, -- 关联 dim_employees.employee_id
    sale_date_id INT NOT NULL, -- 关联 dim_date.date_id
    list_price DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(12, 2),
    final_price DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50) -- e.g., Cash, Finance, Lease
);

-- -----------------------------------------------------
-- 售后服务 (After-Sales)
-- -----------------------------------------------------

-- 服务中心维度表
CREATE TABLE dim_service_centers (
    service_center_id SERIAL PRIMARY KEY,
    service_center_name VARCHAR(150),
    dealership_id INT, -- 关联 dim_dealerships.dealership_id
    is_authorized BOOLEAN,
    city VARCHAR(100),
    address VARCHAR(255)
);

-- 售后服务事实表
CREATE TABLE fact_after_sales_service (
    service_record_id SERIAL PRIMARY KEY,
    vin VARCHAR(17) NOT NULL, -- 关联 dim_vehicles.vin
    customer_id INT NOT NULL, -- 关联 dim_customers.customer_id
    service_center_id INT NOT NULL, -- 关联 dim_service_centers.service_center_id
    technician_id INT, -- 关联 dim_employees.employee_id
    service_date_id INT NOT NULL, -- 关联 dim_date.date_id
    service_type VARCHAR(100), -- e.g., Maintenance, Repair, Recall
    service_description TEXT,
    parts_cost DECIMAL(10, 2),
    labor_cost DECIMAL(10, 2),
    total_cost DECIMAL(10, 2)
); 