# Twitter Data Analysis

This test project divides the data from Twitter into "trending words" to reveal which word is used more frequently.

## Installation

This project uses [Spark](https://spark.apache.org/downloads.html), [Hadoop](https://hadoop.apache.org/releases.html) and [Python](https://www.python.org/downloads/). 

The script file (Version 0.85) in the file has 

```bash
import findspark
findspark.init()
findspark.find()
```
code to locate the computer's installed PySpark.


## Usage
Type the following into the PowerShell or anaconda prompt after downloading one of the twitter_large or twitter_small files and version 0.85.py to your computer:
```python
python version085.py
```
It will enough to alter the following section of the code, regardless of the dataset you wish to use to run the script:
```python
txt = sc.textFile("twitter_small.txt",10).map(lambda x: x.split('": '))
#your choice
txt = sc.textFile("twitter_large.txt",10).map(lambda x: x.split('": '))
```

## Previous Versions and Files
The entire project advanced as I did a little more exploring, applying, and developing version by version:

Version0.01 - Search and results for parsing txt files are displayed first.

Version 0.02: Tokenizing and running the json file through PySpark

Version 0.03: I could obtain word counts but not the proper date format.

Version 0.04: When reading twitter_big, I couldnt read all of the entries.

Version 0.85: I transformed the file into a script and made the large data readable.

*Files*: *twitter_small.txt - 40.376 kb* 
```python
Example Entry:
{"created_at": "Fri May 29 15:38:52 +0000 2020", "id": 1266393580223987713, "id_str": "1266393580223987713", "full_text": "RT @Desiree_Laverne: #MyBeautifulAfrica....\u2764\nAardwolf.. https://t.co/9C7MjkQaIp", "truncated": false, "display_text_range": [0, 79], "entities": {"hashtags": [{"text": "MyBeautifulAfrica", "indices": [21, 39]}], "symbols": [], "user_mentions": [{"screen_name": "Desiree_Laverne", "name": "Desiree_Laverne", "id": 2314937901, "id_str": "2314937901", "indices": [3, 19]}], "urls": [], "media": [{"id": 1266286817315176450, "id_str": "1266286817315176450", "indices": [56, 79], "media_url": "http://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "media_url_https": "https://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "url": "https://t.co/9C7MjkQaIp", "display_url": "pic.twitter.com/9C7MjkQaIp", "expanded_url": "https://twitter.com/Desiree_Laverne/status/1266286830116200448/photo/1", "type": "photo", "sizes": {"thumb": {"w": 150, "h": 150, "resize": "crop"}, "large": {"w": 564, "h": 435, "resize": "fit"}, "small": {"w": 564, "h": 435, "resize": "fit"}, "medium": {"w": 564, "h": 435, "resize": "fit"}}, "source_status_id": 1266286830116200448, "source_status_id_str": "1266286830116200448", "source_user_id": 2314937901, "source_user_id_str": "2314937901"}]}, "extended_entities": {"media": [{"id": 1266286817315176450, "id_str": "1266286817315176450", "indices": [56, 79], "media_url": "http://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "media_url_https": "https://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "url": "https://t.co/9C7MjkQaIp", "display_url": "pic.twitter.com/9C7MjkQaIp", "expanded_url": "https://twitter.com/Desiree_Laverne/status/1266286830116200448/photo/1", "type": "photo", "sizes": {"thumb": {"w": 150, "h": 150, "resize": "crop"}, "large": {"w": 564, "h": 435, "resize": "fit"}, "small": {"w": 564, "h": 435, "resize": "fit"}, "medium": {"w": 564, "h": 435, "resize": "fit"}}, "source_status_id": 1266286830116200448, "source_status_id_str": "1266286830116200448", "source_user_id": 2314937901, "source_user_id_str": "2314937901"}]}, "metadata": {"iso_language_code": "nl", "result_type": "recent"}, "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>", "in_reply_to_status_id": null, "in_reply_to_status_id_str": null, "in_reply_to_user_id": null, "in_reply_to_user_id_str": null, "in_reply_to_screen_name": null, "user": {"id": 294630844, "id_str": "294630844", "name": "BR&ON", "screen_name": "broux73", "location": "Northern Drakensberg \ud83c\uddff\ud83c\udde6\ud83c\udfde\ud83c\udfd5\ufe0f", "description": "Don't just stand there, follow me                                                            |\n\ud83c\udfd4\ufe0f\ud83c\udfd5\ufe0f\u26fa\ud83c\udf04\ud83c\udfde\ufe0f\ud83c\udf3f\ud83c\udfd4\u26f0", "url": null, "entities": {"description": {"urls": []}}, "protected": false, "followers_count": 311, "friends_count": 524, "listed_count": 3, "created_at": "Sat May 07 13:57:02 +0000 2011", "favourites_count": 26933, "utc_offset": null, "time_zone": null, "geo_enabled": true, "verified": false, "statuses_count": 8630, "lang": null, "contributors_enabled": false, "is_translator": false, "is_translation_enabled": false, "profile_background_color": "709397", "profile_background_image_url": "http://abs.twimg.com/images/themes/theme6/bg.gif", "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme6/bg.gif", "profile_background_tile": false, "profile_image_url": "http://pbs.twimg.com/profile_images/1248400674175324160/7ur1k_lb_normal.jpg", "profile_image_url_https": "https://pbs.twimg.com/profile_images/1248400674175324160/7ur1k_lb_normal.jpg", "profile_banner_url": "https://pbs.twimg.com/profile_banners/294630844/1586701202", "profile_link_color": "00FF00", "profile_sidebar_border_color": "86A4A6", "profile_sidebar_fill_color": "A0C5C7", "profile_text_color": "333333", "profile_use_background_image": true, "has_extended_profile": true, "default_profile": false, "default_profile_image": false, "following": false, "follow_request_sent": false, "notifications": false, "translator_type": "none"}, "geo": null, "coordinates": null, "place": null, "contributors": null, "retweeted_status": {"created_at": "Fri May 29 08:34:41 +0000 2020", "id": 1266286830116200448, "id_str": "1266286830116200448", "full_text": "#MyBeautifulAfrica....\u2764\nAardwolf.. https://t.co/9C7MjkQaIp", "truncated": false, "display_text_range": [0, 34], "entities": {"hashtags": [{"text": "MyBeautifulAfrica", "indices": [0, 18]}], "symbols": [], "user_mentions": [], "urls": [], "media": [{"id": 1266286817315176450, "id_str": "1266286817315176450", "indices": [35, 58], "media_url": "http://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "media_url_https": "https://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "url": "https://t.co/9C7MjkQaIp", "display_url": "pic.twitter.com/9C7MjkQaIp", "expanded_url": "https://twitter.com/Desiree_Laverne/status/1266286830116200448/photo/1", "type": "photo", "sizes": {"thumb": {"w": 150, "h": 150, "resize": "crop"}, "large": {"w": 564, "h": 435, "resize": "fit"}, "small": {"w": 564, "h": 435, "resize": "fit"}, "medium": {"w": 564, "h": 435, "resize": "fit"}}}]}, "extended_entities": {"media": [{"id": 1266286817315176450, "id_str": "1266286817315176450", "indices": [35, 58], "media_url": "http://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "media_url_https": "https://pbs.twimg.com/media/EZLBJrnUwAI9IbT.jpg", "url": "https://t.co/9C7MjkQaIp", "display_url": "pic.twitter.com/9C7MjkQaIp", "expanded_url": "https://twitter.com/Desiree_Laverne/status/1266286830116200448/photo/1", "type": "photo", "sizes": {"thumb": {"w": 150, "h": 150, "resize": "crop"}, "large": {"w": 564, "h": 435, "resize": "fit"}, "small": {"w": 564, "h": 435, "resize": "fit"}, "medium": {"w": 564, "h": 435, "resize": "fit"}}}]}, "metadata": {"iso_language_code": "nl", "result_type": "recent"}, "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>", "in_reply_to_status_id": null, "in_reply_to_status_id_str": null, "in_reply_to_user_id": null, "in_reply_to_user_id_str": null, "in_reply_to_screen_name": null, "user": {"id": 2314937901, "id_str": "2314937901", "name": "Desiree_Laverne", "screen_name": "Desiree_Laverne", "location": "\u2764\ud83e\udd8f\ud83c\udf3f", "description": "\u26d4No Dms\u26d4 \n\n\n#Rangers\u2764 \n\n\nACTIVIST \ud83d\udc9a\u270c\n#Resist\n\n#_1_Bullet_1_Poacher. #BanIvoryTrade. \n#BanRhinoHornTrade. #BanCannedHunting.. \n#GMFER", "url": null, "entities": {"description": {"urls": []}}, "protected": false, "followers_count": 7049, "friends_count": 4921, "listed_count": 175, "created_at": "Thu Jan 30 20:56:59 +0000 2014", "favourites_count": 159144, "utc_offset": null, "time_zone": null, "geo_enabled": true, "verified": false, "statuses_count": 163896, "lang": null, "contributors_enabled": false, "is_translator": false, "is_translation_enabled": false, "profile_background_color": "C0DEED", "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png", "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png", "profile_background_tile": false, "profile_image_url": "http://pbs.twimg.com/profile_images/1264906862484492288/iVGLP1O-_normal.jpg", "profile_image_url_https": "https://pbs.twimg.com/profile_images/1264906862484492288/iVGLP1O-_normal.jpg", "profile_banner_url": "https://pbs.twimg.com/profile_banners/2314937901/1585836860", "profile_link_color": "1DA1F2", "profile_sidebar_border_color": "C0DEED", "profile_sidebar_fill_color": "DDEEF6", "profile_text_color": "333333", "profile_use_background_image": true, "has_extended_profile": false, "default_profile": true, "default_profile_image": false, "following": false, "follow_request_sent": false, "notifications": false, "translator_type": "none"}, "geo": null, "coordinates": null, "place": null, "contributors": null, "is_quote_status": false, "retweet_count": 4, "favorite_count": 37, "favorited": false, "retweeted": false, "possibly_sensitive": false, "lang": "nl"}, "is_quote_status": false, "retweet_count": 4, "favorite_count": 0, "favorited": false, "retweeted": false, "possibly_sensitive": false, "lang": "nl"}
 ```
*twitter_big.txt - 3.512.980 kb* 


## What is the script's procedure?
It adds the required libraries after finding the local PySpark installation on the PC.
```python
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
```

Then, this was one of the last issues I ran into, I added configurations to Pyspark so that it doesn't worry about memory space and process redundancy.

```python
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .config("spark.driver.maxResultSize", "5g")\
    .config('spark.executor.instances', 4)\
    .config('spark.executor.cores', 4)\
    .config('spark.executor.memory', '10g')\
    .config('spark.driver.memory', '15g')\
    .config('spark.memory.offHeap.enabled', True)\
    .config('spark.memory.offHeap.size', '20g')\
    .appName('twitter-data') \
    .getOrCreate()
```
After reading the file, check the values in [0][1] and [0][4] to see which lines include the tweet and the date it was posted.
```python
txt.collect()[0][1]
'"Fri May 29 15:38:52 +0000 2020", "id'
txt.collect()[0][4]
'"RT @Desiree_Laverne: #MyBeautifulAfrica....\\u2764\\nAardwolf.. https://t.co/9C7MjkQaIp", "truncated'
```
As soon as I see this, I simply create an RDD map where these rows are retrieved, giving me only the date and the tweet. (distinct only!)

```python
rdd=txt.map(lambda x: (x[1], x[4])).distinct()
```
Then I had to separate the tweet words and get rid of extra words like "and, or," etc. I created a suitable tokenizer, looked at what are the Dutch stop words online, and then filtered the words.
```python
tokenizer2 = Tokenizer(inputCol="Text", outputCol="words_token")
tokenized = tokenizer2.transform(DF1).select('Date','Text','words_token')
remover = StopWordsRemover(stopWords=["aan","af","al","als","bij","dan","dat","die","dit","een","en","er","had","heb","hem","het","hij","hoe","hun","ik","in","is","je","kan","me","men","met","mij","nog","nu","of","ons","ook","te","tot","uit","van","was","wat","we","wel","wij","zal","ze","zei","zij","zo","zou","aangaande","aangezien","achter","achterna","afgelopen","aldaar","aldus","alhoewel","alias","alle","allebei","alleen","alsnog","altijd","altoos","ander","andere","anders","anderszins","behalve","behoudens","beide","beiden","ben","beneden","bent","bepaald","betreffende","binnen","binnenin","boven","bovenal","bovendien","bovengenoemd","bovenstaand","bovenvermeld","buiten","daar","daarheen","daarin","daarna","daarnet","daarom","daarop","daarvanlangs","de","dikwijls","door","doorgaand","dus","echter","eer","eerdat","eerder","eerlang","eerst","elk","elke","enig","enigszins","enkel","erdoor","even","eveneens","evenwel","gauw","gedurende","geen","gehad","gekund","geleden","gelijk","gemoeten","gemogen","geweest","gewoon","gewoonweg","haar","hadden","hare","hebben","hebt","heeft","hen","hierbeneden","hierboven","hoewel","hunne","ikzelf","inmiddels","inzake","jezelf","jij","jijzelf","jou","jouw","jouwe","juist","jullie","klaar","kon","konden","krachtens","kunnen","kunt","later","liever","maar","mag","meer","mezelf","mijn","mijnent","mijner","mijzelf","misschien","mocht","mochten","moest","moesten","moet","moeten","mogen","na","naar","nadat","net","niet","noch","nogal","ofschoon","om","omdat","omhoog","omlaag","omstreeks","omtrent","omver","onder","ondertussen","ongeveer","onszelf","onze","op","opnieuw","opzij","over","overeind","overigens","pas","precies","reeds","rond","rondom","sedert","sinds","sindsdien","slechts","sommige","spoedig","steeds","tamelijk","tenzij","terwijl","thans","tijdens","toch","toen","toenmaals","toenmalig","totdat","tussen","uitgezonderd","vaakwat","vandaan","vanuit","vanwege","veeleer","verder","vervolgens","vol","volgens","voor","vooraf","vooral","vooralsnog","voorbij","voordat","voordezen","voordien","voorheen","voorop","vooruit","vrij","vroeg","waar","waarom","wanneer","want","waren","weer","weg","wegens","weldra","welk","welke","wie","wiens","wier","wijzelf","zelfs","zichzelf","zijn","zijne","zodra","zonder","zouden","zowat","zulke","zullen","zult","rt","ık","deze","u","via","-",",",'"[full_text": ','"truncated', '"rt',' '], inputCol='words_token', outputCol='words_clean')
data_clean = remover.transform(tokenized).select('Date', 'words_clean')
```

Finally, I establish a new database using the cleaned and tokenized words to count the number of times each word is used.

```python
result = data_clean.withColumn('words_clean', f.explode(f.col('words_clean'))) \
  .groupBy('words_clean') \
  .count().sort('count', ascending=False) \
```

Then, I experimented with extra lenses; I believe that it appears in accordance with the time and day, but there is a difference in format when getting the time and date. Although I tried it both ways, using different time conversions, it didn't work.
```python
date_clean.withColumn('date_clean', date_format(f.col("date_clean"),"mm, dd, dd").alias("yyyy MM dd"))

import datetime
def getDate(x):
    if x is not None:
        return str(datetime.strptime(x,'%b, %d, %H:%M:%S +0000').replace(tzinfo=pytz.UTC).strftime("%m-%d %H:%M:%S"))
    else:
        return None
```
Some .ipynb files may appear as not opening because of the fact that there are too many lines, I will explain through the source codes on the day of the presentation.

Umut Mert Demirci
