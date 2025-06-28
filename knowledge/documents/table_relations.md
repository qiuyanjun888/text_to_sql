# 数据库表关系说明

## 主要实体关系

```
customers (客户) 1 -- * sales (销售)
employees (员工) 1 -- * sales (销售) 
stores (门店) 1 -- * sales (销售)
stores (门店) 1 -- * inventory (库存)
stores (门店) 1 -- * employees (员工)
employees (员工) 1 -- * employees (下属员工，自关联)
```

## 详细关系说明

### 客户与销售关系

- 一个客户可以有多个销售记录
- 每个销售记录只对应一个客户
- 连接字段: `customers.customer_id = sales.customer_id`
- 常见查询场景:
  - 查询客户购买历史
  - 计算客户消费总额
  - 分析客户购买偏好

### 员工与销售关系

- 一个员工可以有多个销售记录
- 每个销售记录只对应一个销售员
- 连接字段: `employees.employee_id = sales.sales_person_id`
- 常见查询场景:
  - 计算员工销售业绩
  - 销售员业绩排名
  - 员工提成计算

### 门店与销售关系

- 一个门店可以有多个销售记录
- 每个销售记录只对应一个门店
- 连接字段: `stores.store_id = sales.store_id`
- 常见查询场景:
  - 计算门店销售业绩
  - 门店销售额排名
  - 门店销售趋势分析

### 门店与库存关系

- 一个门店可以有多个库存记录
- 每个库存记录只对应一个门店
- 连接字段: `stores.store_id = inventory.store_id`
- 常见查询场景:
  - 查询门店库存情况
  - 门店间库存比较
  - 库存低的门店分析

### 门店与员工关系

- 一个门店可以有多个员工
- 每个员工只属于一个门店
- 连接字段: `stores.store_id = employees.store_id`
- 常见查询场景:
  - 查询门店员工列表
  - 门店员工绩效分析
  - 门店人力资源规划

### 员工自关联关系(经理与下属)

- 一个经理可以管理多个员工
- 每个员工只有一个直属经理
- 连接字段: `employees.employee_id = employees.manager_id`
- 常见查询场景:
  - 查询员工及其经理
  - 查询经理及其下属
  - 组织架构分析

## 多表联查常见场景

1. 计算每个门店的销售额和库存价值
   - 涉及表: `stores`, `sales`, `inventory`

2. 分析每个销售员的客户消费情况
   - 涉及表: `employees`, `sales`, `customers`

3. 计算每个经理的团队业绩
   - 涉及表: `employees`(自关联), `sales`

4. 统计各地区各车型的销售情况
   - 涉及表: `stores`, `sales`, `inventory`

5. 计算每个客户在不同门店的消费金额
   - 涉及表: `customers`, `sales`, `stores` 