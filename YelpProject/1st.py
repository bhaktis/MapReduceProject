from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# business = sqlContext.read.json("hdfs://localhost:54310/PYTHON/b2.json")

business = sqlContext.jsonFile("hdfs://localhost:54310/PYTHON/business.json")

business.registerTempTable('business')

b = sqlContext.sql("SELECT * FROM business")

c = b.filter(pyspark.sql.functions.array_contains(b.categories , "Restaurants"))


business_rdd = c.map(lambda word: (word.business_id , 1))

# business_rdd.collect()


#--------------------------------------- REVIEW --------------------


review = sqlContext.read.json("hdfs://localhost:54310/PYTHON/review.json")

review.registerTempTable('review')

r = sqlContext.sql("Select * from review")

review_rdd = r.map(lambda word : (word.business_id , word.text))

# review_rdd.collect()

output = business_rdd.join(review_rdd)

output.saveAsTextFile("file:///Users/drodrigues/output.txt")
