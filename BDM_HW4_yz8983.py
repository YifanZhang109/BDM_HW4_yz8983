import csv
import json
import pyspark
import sys

sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

PRODUCTS_FN = '/shared/CUSP-GX-6002/data/keyfood_products.csv'
STORES_FN = 'keyfood_nyc_stores.json'
SAMPLE_FN = 'keyfood_sample_items.csv'

sample_items = sc.textFile(SAMPLE_FN).filter(lambda x: not x.startswith('UPC code,I'))\
      .map(lambda x: x.split(','))\
      .map(lambda x: (x[0].split('-')[1].strip(),x[1]))
products = sc.textFile(PRODUCTS_FN).filter(lambda x: not x.startswith('store,department'))\
      .map(lambda line: next(csv.reader([line])))\
      .filter(lambda x: x[2]!='N/A' and x[5]!='Price not Available')\
      .map(lambda x: (x[2].split('-')[1].strip(), (x[0], float(x[5].split()[0].replace('$', '')))))
stores = sc.textFile(STORES_FN)\
      .map(lambda x: json.loads(x))\
      .flatMap(lambda x: [(x[i]['name'], (x[i]['communityDistrict'], x[i]['foodInsecurity']*100)) for i in x])

outputTask1 = sample_items.join(products)\
      .map(lambda x: (x[1][1][0],(x[1][0],x[1][1][1])))\
      .join(stores)\
      .map(lambda x: (x[1][0][0], x[1][0][1], x[1][1][1]))

## DO NOT EDIT BELOW
outputTask1 = outputTask1.cache()
print(outputTask1.count())
outputTask1.saveAsTextFile(sys.argv[1] if len(sys.argv)>1 else 'output')
