#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark import SparkContext

def main(sc):
    import sys
    file = sys.argv[1]
    def createIndex(shapefile):
        import rtree
        import fiona.crs
        import geopandas as gpd
        zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
        index = rtree.Rtree()
        for idx,geometry in enumerate(zones.geometry):
            index.insert(idx, geometry.bounds)
        return (index, zones)

    def findZone(p, index, zones):
        match = index.intersection((p.x, p.y, p.x, p.y))
        for idx in match:
            if zones.geometry[idx].contains(p):
                return idx
        return None


    def processTrips(pid, records):
        import csv
        import pyproj
        import shapely.geometry as geom

        proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
        index, zones = createIndex('neighborhoods.geojson')

        if pid==0:
            next(records)
        reader = csv.reader(records)
        counts = {}

        for row in reader:
            if len(row)==18:
                try:
                    PickupLong, PickupLat, DropLong, DropLat = float(row[5]), float(row[6]), float(row[9]), float(row[10])
                    pickup = geom.Point(proj(PickupLon, PickupLat))
                    drop = geom.Point(proj(DropLon, DropLat))
                    PickNeighbor = findZone(pickup, index, zones)
                    DropBoro = findZone(drop, index, zones)
                    if PickNeighbor and DropBoro:
                        yield((zones.borough[DropBoro], zones.neighborhood[PickNeighbor]),1)
                except ValueError:
                    pass
    rdd = sc.textFile(file)
    counts = rdd.mapPartitionsWithIndex(processTrips).reduceByKey(lambda x,y:x+y).map(lambda x : (x[0][0],(x[1],x[0][1]))).reduceByKey(lambda x,y:x+(y,)).mapValues(lambda x:(((x[0],x[1]),)+x[2:])).mapValues(lambda x:sorted(x)[-3:]).map(lambda x:('Top 3 neighborhoods to '+ x[0] +' is: ' + x[1][0][1] + ',' + x[1][1][1] + ',' + x[1][2][1] + ',')).collect()
    print(counts)

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)

