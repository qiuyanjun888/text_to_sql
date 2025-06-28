erDiagram
    %% =============================================
    %%  维度表 (Dimension Tables)
    %% =============================================

    dim_departments {
        INT department_id PK "部门ID"
        VARCHAR department_name "部门名称"
        TEXT description "描述"
    }

    dim_employees {
        INT employee_id PK "员工ID"
        VARCHAR first_name "名"
        VARCHAR last_name "姓"
        VARCHAR job_title "职位"
        INT department_id FK "部门ID"
        INT manager_id FK "上级经理ID"
        INT dealership_id FK "经销商ID"
    }

    dim_date {
        INT date_id PK "日期ID (YYYYMMDD)"
        DATE full_date "完整日期"
        INT year "年"
        INT quarter "季度"
        INT month "月"
        INT day "日"
    }

    dim_car_models {
        INT model_id PK "车型ID"
        VARCHAR model_name "车型名称"
        VARCHAR model_code "车型代码"
        VARCHAR category "类别 (SUV,轿车)"
    }

    dim_technologies {
        INT technology_id PK "技术ID"
        VARCHAR technology_name "技术名称"
        VARCHAR field "领域 (电池,自动驾驶)"
    }

    dim_research_projects {
        INT project_id PK "项目ID"
        VARCHAR project_name "项目名称"
        INT start_date_id FK "开始日期ID"
        INT end_date_id FK "结束日期ID"
        INT project_manager_id FK "项目经理ID"
    }

    dim_parts {
        INT part_id PK "零部件ID"
        VARCHAR part_name "零部件名称"
        VARCHAR part_number "零件号"
        VARCHAR category "类别 (引擎,电子)"
    }

    dim_suppliers {
        INT supplier_id PK "供应商ID"
        VARCHAR supplier_name "供应商名称"
        VARCHAR country "国家"
        INT rating "评级"
    }

    dim_factories {
        INT factory_id PK "工厂ID"
        VARCHAR factory_name "工厂名称"
        VARCHAR city "城市"
        INT production_capacity_per_day "日产能"
    }

    dim_vehicles {
        VARCHAR vin PK "车辆识别码"
        INT model_id FK "车型ID"
        VARCHAR color "颜色"
        VARCHAR trim_level "配置级别"
    }

    dim_dealerships {
        INT dealership_id PK "经销商ID"
        VARCHAR dealership_name "经销商名称"
        VARCHAR city "城市"
        INT manager_id FK "经理ID"
        INT dealership_level "经销商级别"
        INT parent_dealership_id FK "上级经销商ID"
    }

    dim_customers {
        INT customer_id PK "客户ID"
        VARCHAR first_name "名"
        VARCHAR last_name "姓"
        VARCHAR city "城市"
        INT first_purchase_date_id FK "首次购买日期ID"
        INT vip_level "VIP等级"
    }

    dim_service_centers {
        INT service_center_id PK "服务中心ID"
        VARCHAR service_center_name "服务中心名称"
        INT dealership_id FK "所属经销商ID"
    }

    %% =============================================
    %%  关联表 (Bridge Tables)
    %% =============================================

    bridge_model_technologies {
        INT model_id PK,FK "车型ID"
        INT technology_id PK,FK "技术ID"
    }

    bridge_project_employees {
        INT project_id PK,FK "项目ID"
        INT employee_id PK,FK "员工ID"
    }

    %% =============================================
    %%  事实表 (Fact Tables)
    %% =============================================

    fact_procurement_orders {
        INT procurement_order_id PK "采购订单ID"
        INT part_id FK "零部件ID"
        INT supplier_id FK "供应商ID"
        INT order_date_id FK "订单日期ID"
        INT quantity "数量"
        DECIMAL total_price "总价"
    }

    fact_production_log {
        INT production_log_id PK "生产记录ID"
        VARCHAR vin FK "车辆识别码"
        INT factory_id FK "工厂ID"
        INT production_date_id FK "生产日期ID"
        BOOLEAN quality_check_passed "质检是否通过"
    }

    fact_parts_inventory {
        INT inventory_snapshot_id PK "库存快照ID"
        INT snapshot_date_id FK "快照日期ID"
        INT part_id FK "零部件ID"
        INT factory_id FK "工厂ID"
        INT quantity_on_hand "现有数量"
    }

    fact_sales {
        INT sale_id PK "销售ID"
        VARCHAR vin FK "车辆识别码"
        INT customer_id FK "客户ID"
        INT dealership_id FK "经销商ID"
        INT salesperson_id FK "销售员ID"
        INT sale_date_id FK "销售日期ID"
        DECIMAL final_price "最终价格"
    }

    fact_after_sales_service {
        INT service_record_id PK "服务记录ID"
        VARCHAR vin FK "车辆识别码"
        INT customer_id FK "客户ID"
        INT service_center_id FK "服务中心ID"
        INT technician_id FK "技术员ID"
        INT service_date_id FK "服务日期ID"
        DECIMAL total_cost "总费用"
    }

    fact_vehicle_inventory {
        INT inventory_snapshot_id PK "库存快照ID"
        INT snapshot_date_id FK "快照日期ID"
        VARCHAR vin FK "车辆识别码"
        INT dealership_id FK "经销商ID"
        INT age_in_days "库龄"
        VARCHAR status "状态 (例如: In Stock)"
    }

    %% =============================================
    %%  关系定义 (Relationships)
    %% =============================================

    dim_departments ||--|{ dim_employees : "包含"
    dim_employees }o--o{ dim_employees : "汇报给"
    dim_research_projects }o--|| dim_date : "开始于"
    dim_research_projects }o--|| dim_date : "结束于"
    dim_research_projects }o--|| dim_employees : "由...管理"
    dim_car_models ||--|{ bridge_model_technologies : "拥有"
    dim_technologies ||--|{ bridge_model_technologies : "应用于"
    dim_research_projects ||--|{ bridge_project_employees : "有"
    dim_employees ||--|{ bridge_project_employees : "参与"
    fact_procurement_orders }o--|| dim_parts : "采购"
    fact_procurement_orders }o--|| dim_suppliers : "来源于"
    fact_procurement_orders }o--|| dim_date : "发生在"
    dim_vehicles }o--|| dim_car_models : "是"
    fact_production_log }o--|| dim_vehicles : "生产"
    fact_production_log }o--|| dim_factories : "在...生产"
    fact_production_log }o--|| dim_date : "于...生产"
    fact_parts_inventory }o--|| dim_parts : "的库存"
    fact_parts_inventory }o--|| dim_factories : "位于"
    fact_parts_inventory }o--|| dim_date : "快照于"
    fact_sales }o--|| dim_vehicles : "销售"
    fact_sales }o--|| dim_customers : "销售给"
    fact_sales }o--|| dim_dealerships : "通过...销售"
    fact_sales }o--|| dim_employees : "由...销售"
    fact_sales }o--|| dim_date : "销售于"
    dim_dealerships }o--|| dim_employees : "由...管理"
    dim_dealerships }o--o{ dim_dealerships : "的上级是"
    dim_customers }o--|| dim_date : "首次购买于"
    dim_service_centers }o--|| dim_dealerships : "属于"
    fact_after_sales_service }o--|| dim_vehicles : "的服务"
    fact_after_sales_service }o--|| dim_customers : "为...服务"
    fact_after_sales_service }o--|| dim_service_centers : "在...服务"
    fact_after_sales_service }o--|| dim_employees : "由...服务"
    fact_after_sales_service }o--|| dim_date : "服务于"
    fact_vehicle_inventory }o--|| dim_date : "快照于"
    fact_vehicle_inventory }o--|| dim_vehicles : "的库存"
    fact_vehicle_inventory }o--|| dim_dealerships : "位于"
 