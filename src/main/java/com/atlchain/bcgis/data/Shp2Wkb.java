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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

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
    public void save(File wkbFile) throws IOException {
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
    }

    /**
     * 将ShapeFile文件中所有Geometry存入GeometryCollection，并转换为byte[]
     * @return
     * @throws IOException
     */
    public byte[] getGeometryBytes() throws IOException {
        ArrayList<Geometry> geometryList = readShpFile();
        Geometry[] geometries = geometryList.toArray(new Geometry[geometryList.size()]);
        GeometryCollection geometryCollection = Utils.getGeometryCollection(geometries);
        byte[] WKBByteArray = Utils.getBytesFromGeometry(geometryCollection);
        return WKBByteArray;
    }

    /**
     * 获取Shapefile文件中的所有空间几何对象
     * @return 包含所有空间几何对象的GeometryCollection
     * @throws IOException
     */
    public ArrayList<Geometry> getGeometry() throws IOException {
        return readShpFile();
    }

    /**
     * 读取Shapefile，将其中所有的空间几何对象保存在GeometryCollection中
     * @return 包含Shapefile中所有空间几何对象的GeometryCollection
     * @throws IOException
     */
    private ArrayList<Geometry> readShpFile() throws IOException {
        // 读取文件数据集
        FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);
        // 从数据集中获取属性源
        SimpleFeatureSource featureSource = store.getFeatureSource();
        // 获取属性
        SimpleFeatureCollection featureCollection = featureSource.getFeatures();
        SimpleFeatureIterator featureIterator = featureCollection.features();
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        while (featureIterator.hasNext()) {
            SimpleFeature feature = featureIterator.next();
            Object geomObj = feature.getDefaultGeometry();
//            System.out.println(geomObj.toString());
            geometryArrayList.add((Geometry) geomObj);
        }
        return geometryArrayList;
    }
}