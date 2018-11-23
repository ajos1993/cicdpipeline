#Databricks notebook source##

### Connect Azure SQL Database ###

jdbcUrl = "jdbc:sqlserver://innovationtestserver.database.windows.net:1433;database=innovationtestdb"

connectionProperties = {
  "user" : "yash",
  "password" : "Qwerty@12345678",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
print()

#### Read [dbo].[tags] from SQL Database ####

tags_df = spark.read.jdbc(jdbcUrl, "tags", properties = connectionProperties) \
                    .select("tagId", "tagName")
tags_df.show()
