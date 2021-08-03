# Hadoop
## 第一次作业-Mapreduce
## 第二次作业-HbaseAPI
### 1. 主要API类和数据模型之间的对应关系
~~~
   java 类 							   	HBase 数据模型
   
   HBaseConfiguration	 					数据库（DataBase)
   
   connection.get获取对应的操作对象
   		HBaseAdmin						表操作对象：表对象创建，删除，查看
   		HTable 							表内容操作对象：表内容的增删改查
   
   HColumnDescriptor（1.X版本）或者ColumnFamilyDescriptorBuilder（2.X版本）：列簇修饰符（Column Qualifier）
   HTableDescriptor（1.X版本）或者TableDescriptorBuilder （2.X版本）		  ：表修饰
   
   四大对数据增删改查api，由HTable对象调用
   		HTable.
   				Put										
   				Get
   				Delete
   				Scan
   处理返回结果集	
   		Result
   		ResultScanner
~~~
### 2. [API DEMO](/src/main/java/com/zzkk/homework02_hbase/HBaseApp.java)
ScanData 截图
![ScanData](/images/02-hbase/getData.png)
