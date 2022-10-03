#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType,StructField, StringType
spark = SparkSession.builder     .master('local[*]')     .config("spark.driver.memory", "15g")     .config("spark.driver.maxResultSize", "5g")    .config('spark.executor.instances', 4)    .config('spark.executor.cores', 4)    .config('spark.executor.memory', '10g')    .config('spark.driver.memory', '15g')    .config('spark.memory.offHeap.enabled', True)    .config('spark.memory.offHeap.size', '20g')    .appName('twitter-data')     .getOrCreate()
sc = spark.sparkContext


# In[ ]:


txt = sc.textFile("twitter_small.txt",10).map(lambda x: x.split('": '))
txt.collect()


# In[ ]:


rdd=txt.map(lambda x: (x[1], x[4])).distinct()


# In[ ]:


deptSchema = StructType([       
    StructField('Date', StringType(), True),
    StructField('Text', StringType(), True)
])


# In[ ]:


DF1 = spark.createDataFrame(rdd, schema = deptSchema)
DF1.show()


# In[ ]:


tokenizer2 = Tokenizer(inputCol="Text", outputCol="words_token")
tokenized = tokenizer2.transform(DF1).select('Date','Text','words_token')
remover = StopWordsRemover(stopWords=["aan","af","al","als","bij","dan","dat","die","dit","een","en","er","had","heb","hem","het","hij","hoe","hun","ik","in","is","je","kan","me","men","met","mij","nog","nu","of","ons","ook","te","tot","uit","van","was","wat","we","wel","wij","zal","ze","zei","zij","zo","zou","aangaande","aangezien","achter","achterna","afgelopen","aldaar","aldus","alhoewel","alias","alle","allebei","alleen","alsnog","altijd","altoos","ander","andere","anders","anderszins","behalve","behoudens","beide","beiden","ben","beneden","bent","bepaald","betreffende","binnen","binnenin","boven","bovenal","bovendien","bovengenoemd","bovenstaand","bovenvermeld","buiten","daar","daarheen","daarin","daarna","daarnet","daarom","daarop","daarvanlangs","de","dikwijls","door","doorgaand","dus","echter","eer","eerdat","eerder","eerlang","eerst","elk","elke","enig","enigszins","enkel","erdoor","even","eveneens","evenwel","gauw","gedurende","geen","gehad","gekund","geleden","gelijk","gemoeten","gemogen","geweest","gewoon","gewoonweg","haar","hadden","hare","hebben","hebt","heeft","hen","hierbeneden","hierboven","hoewel","hunne","ikzelf","inmiddels","inzake","jezelf","jij","jijzelf","jou","jouw","jouwe","juist","jullie","klaar","kon","konden","krachtens","kunnen","kunt","later","liever","maar","mag","meer","mezelf","mijn","mijnent","mijner","mijzelf","misschien","mocht","mochten","moest","moesten","moet","moeten","mogen","na","naar","nadat","net","niet","noch","nogal","ofschoon","om","omdat","omhoog","omlaag","omstreeks","omtrent","omver","onder","ondertussen","ongeveer","onszelf","onze","op","opnieuw","opzij","over","overeind","overigens","pas","precies","reeds","rond","rondom","sedert","sinds","sindsdien","slechts","sommige","spoedig","steeds","tamelijk","tenzij","terwijl","thans","tijdens","toch","toen","toenmaals","toenmalig","totdat","tussen","uitgezonderd","vaakwat","vandaan","vanuit","vanwege","veeleer","verder","vervolgens","vol","volgens","voor","vooraf","vooral","vooralsnog","voorbij","voordat","voordezen","voordien","voorheen","voorop","vooruit","vrij","vroeg","waar","waarom","wanneer","want","waren","weer","weg","wegens","weldra","welk","welke","wie","wiens","wier","wijzelf","zelfs","zichzelf","zijn","zijne","zodra","zonder","zouden","zowat","zulke","zullen","zult","rt","ık","deze","u","via","-",",",'"[full_text": ','"truncated', '"rt',' '], inputCol='words_token', outputCol='words_clean')
data_clean = remover.transform(tokenized).select('Date', 'words_clean')
print('############ Data Cleaning extract:')
data_clean.show()


# In[ ]:


result = data_clean.withColumn('words_clean', f.explode(f.col('words_clean')))   .groupBy('words_clean')   .count().sort('count', ascending=False) 
print('############')
result.show()


# In[ ]:




