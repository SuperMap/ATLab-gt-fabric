package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
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
        SimpleFeatureIterator featureIterator = null;
        try {
            FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);
            SimpleFeatureSource featureSource = store.getFeatureSource();
            store.dispose();
            SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            featureIterator = featureCollection.features();
            while (featureIterator.hasNext()) {
                SimpleFeature feature = featureIterator.next();
                Object geomObj = feature.getDefaultGeometry();
                geometryArrayList.add((Geometry) geomObj);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        featureIterator.close();

        return geometryArrayList;
    }

    /**
     * 获取 shpfile 文件属性
     * @return
     */
    public JSONArray getShpFileAttributes()  {
        JSONArray jsonArray = new JSONArray();
        try {
            FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);
            // GeoTools读取ShapeFile文件的默认编码为ISO-8859-1。而我们中文操作系统下ShapeFile文件的默认编码一般为utf-8
            ((ShapefileDataStore) store).setCharset(Charset.forName("utf-8"));
            SimpleFeatureSource featureSource = store.getFeatureSource();
            store.dispose();
            // 提取属性 ID
            List<String> attributeID = new LinkedList<>();
            List<AttributeDescriptor> attrList= featureSource.getSchema().getAttributeDescriptors();
            for(AttributeDescriptor attr : attrList){
                String ID = attr.getName().getLocalPart();
                if( !ID.equals("the_geom")){
                    attributeID.add(ID);
                }
            }
            SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            SimpleFeatureIterator featureIterator = featureCollection.features();
            int i = 0;
            List<Object> list;
            while (featureIterator.hasNext()) {
                SimpleFeature feature = featureIterator.next();
                JSONObject jsonObject = new JSONObject();
                list = feature.getAttributes();
                for(int k = 1; k < list.size(); k++ ){
                    Object o = list.get(k);
                    if(o == null || o.toString().length() == 0){
                        o = "null";
                    }
                    jsonObject.put(attributeID.get(k-1),  o);
                }
                jsonArray.add(jsonObject);
                i++;
                if(String.valueOf(i).equals("247")){
                    System.out.println(i);
                }
            }
            featureIterator.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
}