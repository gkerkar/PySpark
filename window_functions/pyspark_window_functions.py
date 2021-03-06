# -*- coding: utf-8 -*-
"""
PySpark_Window_Functions.ipynb

"""

!pip install pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkWindowFunctionsExamples').getOrCreate()

# Create sample dataframe

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

df.createOrReplaceTempView("employees")

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number, rank, dense_rank, lag, lead, col, avg, sum, min, max

"""**row_number Window Function**

row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.
"""

# Partition by Department and ascending salary.

df.withColumn("row_number",row_number().over(Window.partitionBy("department").orderBy("salary"))) \
    .show()

spark.sql("SELECT employee_name, department, salary, \
          row_number() over (partition by department order by salary) row_number \
          FROM employees").show()

# Partition by Department and descending salary.

df.withColumn("row_number",row_number().over(Window.partitionBy("department").orderBy(desc("salary")))) \
    .show()

spark.sql("SELECT employee_name, department, salary, \
          row_number() over (partition by department order by salary desc) row_number \
          FROM employees").show()

"""**rank Window Function**

rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
"""

# Partition by Department and ascending salary.

df.withColumn("rank",rank().over(Window.partitionBy("department").orderBy("salary"))).show()

spark.sql("SELECT employee_name, department, salary, rank() over(partition by department order by salary) rank \
            FROM employees")\
            .show()

# Partition by Department and descending salary.

df.withColumn("rank",rank().over(Window.partitionBy("department").orderBy(desc("salary")))).show()

spark.sql("SELECT employee_name, department, salary, rank() over(partition by department order by salary desc) rank \
            FROM employees")\
            .show()

"""**dense_rank Window**

dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
"""

# Partition by Department and ascending salary.

df.withColumn("rank",dense_rank().over(Window.partitionBy("department").orderBy("salary"))).show()

spark.sql("SELECT employee_name, department, salary, dense_rank() over(partition by department order by salary) dense_rank \
          FROM employees") \
          .show()

# Partition by Department and descending salary.

df.withColumn("rank",dense_rank().over(Window.partitionBy("department").orderBy(desc("salary")))).show()

spark.sql("SELECT employee_name, department, salary, dense_rank() over(partition by department order by salary desc) dense_rank \
          FROM employees") \
          .show()

"""**lag Window Function**

This is the same as the LAG function in SQL.
"""

# Partition by Department and ascending salary.

df.withColumn("lag",lag("salary").over(Window.partitionBy("department").orderBy("salary"))).show()

spark.sql("SELECT employee_name, department, salary, lag(salary) over(partition by department order by salary) lag \
            FROM employees") \
            .show()

df.withColumn("lag",lag("salary",2,0).over(Window.partitionBy("department").orderBy("salary"))).show()

spark.sql("SELECT employee_name, department, salary, lag(salary,2,0) over(partition by department order by salary) lag \
            FROM employees") \
            .show()

# Partition by Department and descending salary.

df.withColumn("lag",lag("salary").over(Window.partitionBy("department").orderBy(desc("salary")))).show()

spark.sql("SELECT employee_name, department, salary, lag(salary) over(partition by department order by salary desc) lag \
            FROM employees") \
            .show()

df.withColumn("lag",lag("salary",2,0).over(Window.partitionBy("department").orderBy(desc("salary")))).show()

spark.sql("SELECT employee_name, department, salary, lag(salary,2,0) over(partition by department order by salary desc) lag \
            FROM employees") \
            .show()

"""**lead Window Function**

This is the same as the LEAD function in SQL.
"""

# Partition by Department and ascending salary.

df.withColumn("lead",lead("salary").over(Window.partitionBy("department").orderBy("salary"))).show()

spark.sql("SELECT employee_name, department, salary, lead(salary) over(partition by department order by salary) lead \
          FROM employees") \
          .show()

df.withColumn("lead",lead("salary",2,0).over(Window.partitionBy("department").orderBy("salary"))).show()

spark.sql("SELECT employee_name, department, salary, lead(salary,2,0) over(partition by department order by salary) lead \
          FROM employees") \
          .show()

# Partition by Department and descending salary.

df.withColumn("lead",lead("salary").over(Window.partitionBy("department").orderBy(desc("salary")))).show()

spark.sql("SELECT employee_name, department, salary, lead(salary) over(partition by department order by salary desc) lead \
          FROM employees") \
          .show()

df.withColumn("lead",lead("salary",2,0).over(Window.partitionBy("department").orderBy(desc("salary")))).show()

spark.sql("SELECT employee_name, department, salary, lead(salary,2,0) over(partition by department order by salary desc) lead \
          FROM employees") \
          .show()

"""**PySpark Window Aggregate Functions**

In this section, I will explain how to calculate sum, min, max for each department using PySpark SQL Aggregate window functions and WindowSpec. When working with Aggregate functions, we don’t need to use order by clause.
"""

df.withColumn("row",row_number().over(Window.partitionBy("department").orderBy("salary")))\
  .withColumn("max_dept_sal",max("salary").over(Window.partitionBy("department")))\
  .withColumn("min_dept_sal",min("salary").over(Window.partitionBy("department")))\
  .withColumn("avg_dept_sal",avg("salary").over(Window.partitionBy("department")))\
  .withColumn("sum_dept_sal",sum("salary").over(Window.partitionBy("department")))\
  .where(col("row")==1)\
  .select("department", "avg_dept_sal", "sum_dept_sal", "min_dept_sal", "max_dept_sal")\
  .show()

spark.sql("SELECT department, avg(salary) as avg_dept_sal \
          , sum(salary) as sum_dept_sal \
          , min(salary) as min_dept_sal \
          , max(salary) as max_dept_sal \
          FROM employees \
          GROUP BY department \
          ") \
        .show()
