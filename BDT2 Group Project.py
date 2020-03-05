# Databricks notebook source
# DBTITLE 1,Importing all csv's
filePath = "/FileStore/tables/GroupData/" #put your own file path if necessary

#Importing the files one by one
Complaints = spark.read\
  .format("csv")\
  .option("inferSchema","true")\
  .option("header","true")\
  .option("delimiter",",")\
  .option("0","NA")\
  .load(filePath + "BDT2_1920_Complaints.csv")\

Delivery=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Delivery.csv")

Subscriptions=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Subscriptions.csv")

Customers=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Customers.csv")

Formula=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Formula.csv")

# COMMAND ----------

# DBTITLE 1,Replacing NAs
from pyspark.sql.functions import *

#Replacing NA in Complaints
#replacing NA with meaningfull value when possible.
#unknown ID will take the value 0
#unknown numeric values like quantities will take the value 999
#NA values in string type column will take a "NA" value or a "no response"/"no solution" depending on the context

#Replacing NA in Complaints
Complaints = Complaints.withColumn("ProductID", when(Complaints["ProductID"] == "NA", 0).otherwise(Complaints["ProductID"]))\
  .withColumn("ProductName", when(Complaints["ProductName"] == "NA", "NA").otherwise(Complaints["ProductName"]))\
  .withColumn("FeedbackTypeID", when(Complaints["FeedbackTypeID"] == "NA", 0).otherwise(Complaints["FeedbackTypeID"]))\
  .withColumn("FeedbackTypeDesc", when(Complaints["FeedbackTypeDesc"] == "NA", "no response").otherwise(Complaints["FeedbackTypeDesc"]))\
  .withColumn("SolutionTypeID", when(Complaints["SolutionTypeID"] == "NA", 0).otherwise(Complaints["SolutionTypeID"]))\
  .withColumn("SolutionTypeDesc", when(Complaints["SolutionTypeDesc"] == "NA", "no solution").otherwise(Complaints["SolutionTypeDesc"]))

#Replacing NA in Delivery
Delivery = Delivery.na.fill("NA", "DeliveryClass")

#Replacing NA in Subscriptions
#NbrMeals_EXCEP NAs have been replaced by the mean NbrMeals_EXCEP ordered by the same NbrMeals_REG category
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==76), 12).otherwise(Subscriptions["NbrMeals_EXCEP"]))
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==304), 13).otherwise(Subscriptions["NbrMeals_EXCEP"]))
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==329), 17).otherwise(Subscriptions["NbrMeals_EXCEP"]))
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==152), 13).otherwise(Subscriptions["NbrMeals_EXCEP"]))

#RenewalDate 1 and 0 (so if a client renewed 6 times his subscription, the value can be summed to 6)
Subscriptions = Subscriptions.withColumn("RenewalDate",when(Subscriptions["RenewalDate"] == "NA",0).otherwise(1))

#PaymentDate Redondant with PaymentStatus
  #GrossFormulaPrice
  #NetFormulaPrice
  #NbrMealsPrice
  #ProductDiscount
  #FormulaDiscount
  #TotalDiscount
  #TotalPrice
  #TotalCredit
  #All of those are codependent. Maybe after grouping the NbrMeals_REG/EXCEP features, we can replace NA's by the mean of 
  # the category they belong to


# COMMAND ----------

# DBTITLE 1,Changing column types after replacing NAs
#Complaints
Complaints = Complaints.withColumn("ProductID", Complaints["ProductID"].cast("integer"))\
  .withColumn("SolutionTypeID", Complaints["SolutionTypeID"].cast("integer"))\
  .withColumn("FeedbackTypeID", Complaints["FeedbackTypeID"].cast("integer"))

#Subscriptions
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",Subscriptions["NbrMeals_EXCEP"].cast("integer"))
Subscriptions = Subscriptions.withColumn("RenewalDate",Subscriptions["RenewalDate"].cast("integer"))
  #converting timestamps to number of days
Subscriptions = Subscriptions.withColumn("EndDate",Subscriptions["EndDate"].cast("long")/86400)
Subscriptions = Subscriptions.withColumn("StartDate",Subscriptions["StartDate"].cast("long")/86400)
Complaints = Complaints.withColumn("ComplaintDate",Complaints["ComplaintDate"].cast("long")/86400)

# COMMAND ----------

# DBTITLE 1,Creating new Columns/Features
#Subscriptions
Subscriptions = Subscriptions.withColumn("SubscriptionDuration", Subscriptions.EndDate - Subscriptions.StartDate)
Subscriptions = Subscriptions.withColumn("NbrMealsPerDay", when(Subscriptions["SubscriptionDuration"] == 0, Subscriptions.NbrMeals_REG).otherwise(Subscriptions.NbrMeals_REG / Subscriptions.SubscriptionDuration))

# COMMAND ----------

display(Subscriptions)

# COMMAND ----------

Subscriptions.createOrReplaceTempView("subscriptions")

# COMMAND ----------

SubInter = spark.sql("select CustomerID, sum(NbrMeals_REG) as TotalMeal_REG, avg(NbrMeals_REG) as MeanMeal_REGPerSub, sum(NbrMeals_EXCEP) as TotalMeal_EXCEP, avg(NbrMeals_EXCEP) as MeanMeal_EXCEPPerSub, min(StartDate) as FirstSubDate, max(EndDate) as EndOfLastSub, (max(EndDate)-min(StartDate)) as HasBeenClientForXDays,count(SubscriptionID) as NbrSub, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END) as SubPaid, SUM(CASE WHEN PaymentStatus='Not Paid' THEN 1 ELSE 0 END) as SubNotPaid, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END)/count(SubscriptionID) as ProportionPaidSub,avg(NbrMealsPrice) as AvgPricePerMeal, sum(ProductDiscount) as TotalProductDiscount, sum(FormulaDiscount) as TotalFormulaDiscount, sum(TotalDiscount) as TotalDiscount, sum(TotalPrice) as TotalPrice, sum(TotalCredit) as TotalCredit,sum(SubscriptionDuration) as NbrDaysSub, avg(SubscriptionDuration) as AvgDurationPerSub, avg(NbrMealsPerDay) as AverageNbrMealPerDay, SUM(CASE WHEN ProductName='Custom Events' THEN 1 ELSE 0 END) as NbrCustomEventsProduct, SUM(CASE WHEN ProductName!='Custom Events' THEN 1 ELSE 0 END) as NbrGrubProduct from subscriptions group by CustomerID")
SubInter = SubInter.withColumn("FirstSubDate", SubInter.FirstSubDate*86400)
SubInter = SubInter.withColumn("FirstSubDate", SubInter.FirstSubDate.cast("timestamp"))
SubInter = SubInter.withColumn("EndOfLastSub", SubInter.EndOfLastSub*86400)
SubInter = SubInter.withColumn("EndOfLastSub", SubInter.EndOfLastSub.cast("timestamp"))
#SubInter = SubInter.withColumn("FirstSubDate", SubInter.select((unix_timestamp("FirstSubDate","yyy/MM/dd HH:mm:ss")).cast("timestamp")))
#df.select((unix_timestamp($"Date", "MM/dd/yyyy HH:mm:ss") * 1000).cast("timestamp"), $"Date")

#creating the churn dependent variable
SubInter = SubInter.withColumn("ChurnedAt03/02/2019", when(col("EndOfLastSub") > "2019-02-02 00:00:00", 0).otherwise(1))
SubInter = SubInter.withColumn("ChurnedAt03/08/2018", when(col("EndOfLastSub") > "2018-08-02 00:00:00", 0).otherwise(1))
SubInter = SubInter.withColumn("ChurnedAt03/02/2018", when(col("EndOfLastSub") > "2018-02-02 00:00:00", 0).otherwise(1))

display(SubInter)

# COMMAND ----------

Complaints.createOrReplaceTempView("complaints")

# COMMAND ----------

Intermediary = spark.sql("select CustomerID, count(ComplaintID) as NbrComplaints, max(ComplaintDate) as LastComplaint, min(ComplaintDate) as FirstComplaint, (CASE WHEN count(ComplaintID)>1 THEN (count(ComplaintID)/(max(ComplaintDate)-min(ComplaintDate))) ELSE 0 END) as ComplaintsPerMonth, SUM(CASE WHEN ProductID=1 THEN 1 ELSE 0 END) as NbrComplaintsProduct1, SUM(CASE WHEN ProductID=2 THEN 1 ELSE 0 END) as NbrComplaintsProduct2, SUM(CASE WHEN ProductID=3 THEN 1 ELSE 0 END) as NbrComplaintsProduct3, SUM(CASE WHEN ProductID=4 THEN 1 ELSE 0 END) as NbrComplaintsProduct4, SUM(CASE WHEN ProductID=5 THEN 1 ELSE 0 END) as NbrComplaintsProduct5, SUM(CASE WHEN ProductID=6 THEN 1 ELSE 0 END) as NbrComplaintsProduct6, SUM(CASE WHEN ProductID=7 THEN 1 ELSE 0 END) as NbrComplaintsProduct7 , SUM(CASE WHEN ProductID=8 THEN 1 ELSE 0 END) as NbrComplaintsProduct8, SUM(CASE WHEN ProductID=0 THEN 1 ELSE 0 END) as NbrComplaintsProductUnknown,SUM(CASE WHEN ComplaintTypeID=1 THEN 1 ELSE 0 END) as NbrComplaintsType1, SUM(CASE WHEN ComplaintTypeID=2 THEN 1 ELSE 0 END) as NbrComplaintsType2, SUM(CASE WHEN ComplaintTypeID=3 THEN 1 ELSE 0 END) as NbrComplaintsType3, SUM(CASE WHEN ComplaintTypeID=4 THEN 1 ELSE 0 END) as NbrComplaintsType4, SUM(CASE WHEN ComplaintTypeID=5 THEN 1 ELSE 0 END) as NbrComplaintsType5, SUM(CASE WHEN ComplaintTypeID=6 THEN 1 ELSE 0 END) as NbrComplaintsType6, SUM(CASE WHEN ComplaintTypeID=7 THEN 1 ELSE 0 END) as NbrComplaintsType7 , SUM(CASE WHEN ComplaintTypeID=8 THEN 1 ELSE 0 END) as NbrComplaintsType8, SUM(CASE WHEN ComplaintTypeID=9 THEN 1 ELSE 0 END) as NbrComplaintsType9, SUM(CASE WHEN ComplaintTypeID=0 THEN 1 ELSE 0 END) as NbrComplaintsTypeUnknown, SUM(CASE WHEN SolutionTypeID=1 THEN 1 ELSE 0 END) as NbrSolutionsType1, SUM(CASE WHEN SolutionTypeID=2 THEN 1 ELSE 0 END) as NbrSolutionsType2, SUM(CASE WHEN SolutionTypeID=3 THEN 1 ELSE 0 END) as NbrSolutionsType3, SUM(CASE WHEN SolutionTypeID=4 THEN 1 ELSE 0 END) as NbrSolutionsType4, SUM(CASE WHEN SolutionTypeID=0 THEN 1 ELSE 0 END) as NbrSolutionsTypeUnknown from complaints group by CustomerID")
Intermediary = Intermediary.withColumn("FirstComplaint", Intermediary.FirstComplaint*86400)
Intermediary = Intermediary.withColumn("FirstComplaint", Intermediary.FirstComplaint.cast("timestamp"))
Intermediary = Intermediary.withColumn("LastComplaint", Intermediary.LastComplaint*86400)
Intermediary = Intermediary.withColumn("LastComplaint", Intermediary.LastComplaint.cast("timestamp"))

display(Intermediary)

# COMMAND ----------

display(SubInter)

# COMMAND ----------

# DBTITLE 1,Merging tables
#Base Table
#base = Customers.join(Complaints,on=['CustomerID'],how='full')
#base = Customers.join(Subscriptions,on=['CustomerID'],how='full')
base = Customers.join(Intermediary,on=['CustomerID'],how='full')
base1 = base.join(SubInter,on=['CustomerID'],how='full')
#Replacing null in Complaints
base1 = base1.na.fill(0)

# COMMAND ----------

display(base1)
