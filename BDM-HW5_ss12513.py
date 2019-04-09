# coding: utf-8
from pyspark import SparkContext 
def main(sc):
    import sys
    file = sys.argv[1]
    def processTrips(pid,records):
        if pid==0:
            next(records)
        counts = {}
        import rtree
        import geopandas as gpd
        import fiona.crs
        import csv 
        import pyproj
        import shapely.geometry as geom
        reader = csv.reader(records)
        counts={}
        proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
        #boroughs = 'boroughs.geojson'
        boroughs = 'hdfs:///tmp/bdm/boroughs.geojson'
        boroughs = gpd.read_file(boroughs).to_crs(fiona.crs.from_epsg(2263))
        bor_index = rtree.Rtree()
        for idx,geometry in enumerate(boroughs.geometry):
            bor_index.insert(idx, geometry.bounds)
        #neighborhoods = 'neighborhoods.geojson'
        neighborhoods = 'hdfs:///tmp/bdm/neighborhoods.geojson'
        nbs = gpd.read_file(neighborhoods).to_crs(fiona.crs.from_epsg(2263))
        nei_index = rtree.Rtree()
        for idx,geometry in enumerate(nbs.geometry):
            nei_index.insert(idx, geometry.bounds)
        for row in reader:
            try :
                p_end = geom.Point(proj(float(row[5]), float(row[4])))
                p_start = geom.Point(proj(float(row[3]), float(row[2])))
            except:
                continue 
            for idx in bor_index.intersection((p_end.x, p_end.y, p_end.x, p_end.y)):
                if boroughs.geometry[idx].contains(p_end):
                    borough = boroughs['boroname'][idx]                
                    for idx2 in nei_index.intersection((p_start.x, p_start.y, p_start.x, p_start.y)):
                        if nbs.geometry[idx2].contains(p_start):
                                    neigh = nbs['neighborhood'][idx2]
                                    key = neigh + "_" + borough
                                    counts[key] = counts.get(key, 0) + 1
        return counts.items()      
    def toCSV(row):
        return ','.join(str(r) for r in row)
    rdd = sc.textFile(file)
    rdd.mapPartitionsWithIndex(processTrips).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0].split('_')[1],[(x[0].split('_')[0],x[1])])).reduceByKey(lambda x,y: sorted(x+y,key=lambda y: -y[1]) if len(x+y)<=3 else sorted(x+y,key=lambda y: -y[1])[:3]).map(toCSV).saveAsTextFile('HW5.csv')
if __name__ == "__main__":
    sc = SparkContext()
 # Execute the main function
    main(sc)

