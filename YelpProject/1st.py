from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# business = sqlContext.read.json("hdfs://localhost:54310/PYTHON/b2.json")

business = sqlContext.jsonFile("hdfs://localhost:54310/PYTHON/business.json")

business.registerTempTable('business')

b = sqlContext.sql("SELECT business_id, name, neighborhoods, full_address, city, state, stars, categories FROM business")

b.printSchema()

c = b.filter(pyspark.sql.functions.array_contains(b.categories , "Restaurants"))

business_rdd = c.map(lambda word: (word.business_id , [word.name, word.full_address, word.neighborhoods,
                                   word.city, word.state, word.stars]))




#--------------------------------------- REVIEW --------------------


review = sqlContext.read.json("hdfs://localhost:54310/PYTHON/review.json")

review.registerTempTable('review')

r = sqlContext.sql("Select business_id, stars, text from review")

r.printSchema()

review_rdd = r.map(lambda word : (word.business_id , [word.text, word.stars]))

output = business_rdd.join(review_rdd)

output.saveAsTextFile("file:///Users/drodrigues/output.txt")










#b = business.select('business_id' , 'categories');

# b.filter(pyspark.sql.functions.array_contains(b.categories , "Restaurants")).show()


# review = sqlContext.read.json("hdfs://localhost:54310/PYTHON/r3.json")

# r = review.select('business_id' , 'text')

# j = b.join(r , b.business_id == r.business_id)

# j.show()

#joinData.show()

#business_data = business.map(lambda x : x.getString("business_id"))

#business_data.collect()

#business.select(business['business_id' , business'name']).show()


