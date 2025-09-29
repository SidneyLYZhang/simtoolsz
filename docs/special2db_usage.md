# special2db 函数使用指南

`special2db` 函数用于将特殊格式的数据文件（TSV、Avro、Arrow）转换为DuckDB数据库。

## 支持的文件格式

- **TSV**: 制表符分隔的文本文件
- **Avro**: Apache Avro格式文件  
- **Arrow**: Apache Arrow格式文件

## 基本用法

### 读取单个TSV文件

```python
from simtoolsz.db import special2db

# 读取单个TSV文件到DuckDB
con = special2db('data/users.tsv', 'users.db')

# 查询数据
tables = con.execute("SHOW TABLES").fetchall()
count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
print(f"记录数: {count}")

# 关闭连接
con.close()
```

### 使用自定义表名

```python
# 使用自定义表名
con = special2db('data/customers.tsv', 'customers.db', table='客户表')

# 查询自定义表名的数据
count = con.execute("SELECT COUNT(*) FROM 客户表").fetchone()[0]
print(f"客户表记录数: {count}")
con.close()
```

### 处理目录中的多个文件

```python
# 处理目录中的所有TSV文件
con = special2db('data_directory', 'all_data.db')

# 查看所有创建的表
tables = con.execute("SHOW TABLES").fetchall()
print(f"创建的表: {[table[0] for table in tables]}")

# 查询特定表
count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
print(f"users表记录数: {count}")
con.close()
```

### 指定读取参数

```python
# 指定编码和其他参数
con = special2db('data/data.tsv', 'output.db', 
                encoding='utf-8', 
                header=True)

# 查询数据
tables = con.execute("SHOW TABLES").fetchall()
print(f"创建的表: {[table[0] for table in tables]}")
con.close()
```

## 错误处理

函数会在以下情况下抛出 `ValueError`：

1. **找不到支持的数据文件**
   ```python
   # 目录中没有TSV、Avro或Arrow文件
   con = special2db('empty_directory', 'output.db')
   # ValueError: 未找到支持的数据文件（tsv、avro、arrow）
   ```

2. **不支持的文件格式**
   ```python
   # 尝试读取不支持的文件格式
   con = special2db('data/file.txt', 'output.db')
   # ValueError: 不支持的文件格式: .txt。支持的格式: tsv, avro, arrow
   ```

## 表名处理规则

1. **默认表名**: 使用文件名（不含扩展名）
2. **自定义表名**: 通过 `table` 参数指定
3. **表名清理**: 自动移除特殊字符，只保留字母、数字和下划线
4. **多文件处理**: 每个文件创建一个表，表名基于文件名

## 注意事项

1. **数据库连接**: 函数返回DuckDB连接对象，使用完毕后需要手动关闭
2. **表覆盖**: 如果表已存在，会先删除再重新创建
3. **文件锁定**: 确保在使用完连接后关闭，避免文件锁定问题
4. **临时文件**: 示例中使用临时文件时，注意文件生命周期管理

## 完整示例

参考 `examples/special2db_example.py` 文件获取完整的使用示例。