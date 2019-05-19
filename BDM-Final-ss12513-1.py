from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SQLContext
import pandas as pd
def main(sc):
    import sys
    file = sys.argv[1]
    def Special(string):
         return string.encode('ascii', 'ignore').decode('ascii')
    def findTract(point, tracts_index, tracts):
            idx = tracts_index.intersection((point.x, point.y, point.x, point.y))
            for i in idx:
                if tracts.geometry[i].is_valid:
                    if tracts.geometry[i].contains(point):
                        try:
                            pop = int(tracts.plctrpop10[i])
                            if pop > 0:
                                return (tracts.plctract10[i], pop)
                        except:
                            pass
                    return None
    def findBoundary():
        import rtree
        import geopandas as gpd
        import fiona.crs
        import csv 
        import pyproj
        import shapely.geometry as geom
        tracts = gpd.read_file('500cities_tracts.geojson')
        tracts_index = rtree.Rtree()
        for idx,geometry in enumerate(tracts.geometry):
            tracts_index.insert(idx, geometry.bounds)
        return tracts,tracts_index
    def toCSVLine(data):
          return ','.join(str(d) for d in data)
    def mapper1(rows):
        import csv
        import re
        import rtree
        import geopandas as gpd
        import fiona.crs
        import csv 
        import pyproj
        import shapely.geometry as geom
        textfile = open("drug_illegal.txt", "r")
        textfile2 = open("drug_sched2.txt","r")
        keys= [line.rstrip().lower() for line in textfile.readlines()]
        keys2 = [line.rstrip().lower() for line in textfile2.readlines()]
        keywords = keys + keys2
        firstwords = [word.split(' ')[0].lower() for word in keywords]
        tracts,tracts_index = findBoundary()
        reader = csv.reader(rows,delimiter = "|")
        for row1 in reader:
            if (row1[5] != "") & (row1[2]!= "") & (row1[1] != ""):
                words = Special(row1[5]).lower()
                found = False
                check = row1[5].lower().split(' ')
                check = [re.sub(r'[^\w\s]','',word) for word in check]
                check = set(check)
                if (check.intersection(set(firstwords)) != set()):
                    for key in keywords:
                        k = " " + key + " " 
                        if k in " " + words+ " ":
                            found = True
                            break
                    if (found):
                        try:
                            p = geom.Point(float(row1[2]),float(row1[1]))
                            tract = findTract(p, tracts_index, tracts)
                            if (tract != None):
                                yield (tract,1)

                        except:
                            pass
                    #   yield ((Special(row1[5]),(row1[2],row1[1])))"""
    rdd = sc.textFile(file)
    result = rdd.mapPartitions(mapper1).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0][0],float(x[1]/x[0][1]))).sortByKey().collect()
    pd.DataFrame(result).to_csv('out.csv')
#    print(result.collect())
#    save = result.map(toCSVLine)
#    save.coalesce(1).saveAsTextFile('hdfs:///tmp/output12513')
#    #save.saveAsSingleTextFile('hdfs:///tmp/output12513.csv')
#    print("Finished")
if __name__ == "__main__":
    sc = SparkContext()
    
 # Execute the main function
    main(sc)