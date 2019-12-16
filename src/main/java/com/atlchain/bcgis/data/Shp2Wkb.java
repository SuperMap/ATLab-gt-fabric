package com.atlchain.bcgis.data;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Shp2Wkb {
    private File shpFile = null;

    /**
     * Shp2Wkb
     * @param shpFile Shapefile文件
     */
    public Shp2Wkb(File shpFile) {
        this.shpFile = shpFile;
    }

    /**
     * 将Shapefile中的空间几何对象保存到WKB文件
     * @param wkbFile WKB文件
     * @throws IOException
     */
    public void save(File wkbFile){
        try {
            if (!wkbFile.exists()) {
                wkbFile.createNewFile();
            }
            byte[] WKBByteArray = getGeometryBytes();
            FileOutputStream out = null;
            try {
                out = new FileOutputStream(wkbFile);
                out.write(WKBByteArray);
            } finally {
                out.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将ShapeFile文件中所有Geometry存入GeometryCollection，并转换为byte[]
     * @return
     */
    public byte[] getGeometryBytes(){
        ArrayList<Geometry> geometryList = readShpFile();
        Geometry[] geometries = geometryList.toArray(new Geometry[geometryList.size()]);
        GeometryCollection geometryCollection = Utils.getGeometryCollection(geometries);
        byte[] WKBByteArray = Utils.getBytesFromGeometry(geometryCollection);
        return WKBByteArray;
    }

    /**
     * 获取Shapefile文件中的所有空间几何对象
     * @return 包含所有空间几何对象的GeometryCollection
     */
    public ArrayList<Geometry> getGeometry(){
        return readShpFile();
    }

    /**
     * 读取Shapefile，将其中所有的空间几何对象保存在GeometryCollection中
     * @return 包含Shapefile中所有空间几何对象的GeometryCollection
     */
    private ArrayList<Geometry> readShpFile()  {
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        try {
            FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);
            SimpleFeatureSource featureSource = store.getFeatureSource();
            SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            SimpleFeatureIterator featureIterator = featureCollection.features();
            while (featureIterator.hasNext()) {
                SimpleFeature feature = featureIterator.next();
                Object geomObj = feature.getDefaultGeometry();
                geometryArrayList.add((Geometry) geomObj);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return geometryArrayList;
    }

    /**
     * 获取 shpfile 文件属性
     * @return
     */
    public List<String> getShpFileAttributes()  {
        List<String> list = new ArrayList<>();
        try {
            FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);
            SimpleFeatureSource featureSource = store.getFeatureSource();
            SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            SimpleFeatureIterator featureIterator = featureCollection.features();
            while (featureIterator.hasNext()) {
                SimpleFeature feature = featureIterator.next();
                String s = feature.getAttribute("name").toString();
//                String s = "D";
                list.add(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}