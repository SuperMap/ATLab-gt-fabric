package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONObject;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.styling.SLD;
import org.geotools.styling.Style;
import org.geotools.swing.JMapFrame;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class BCGISDataStoreTest {
    private String shpURL = this.getClass().getResource("/chenduqu/chengduqu.shp").getFile();  //  /Province/Province_R.shp  /D/D.shp  /chenduqu/chenduqu.shp
    private File shpFile = new File(shpURL);
    private String chaincodeName = "bcgiscc";
    private String functionName = "GetRecordByKey";
    private String recordKey = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4"; // 23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8
    //   D             6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4
    //  中国地图        23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8
    //  成都区            5668c664c852b2b95543b784371f0267136cb4e09b8cb4a284148d2b9f578301
    private File networkFile = new File(this.getClass().getResource("/network-config-test.yaml").getPath());
    private BCGISDataStore bcgisDataStore = new BCGISDataStore(
            networkFile,
            chaincodeName,
            functionName,
            recordKey
    );

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    // test FeatureStore read function
    @Test
    public void testFeatureSource() throws IOException {
        ContentFeatureSource featureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        System.out.println(featureSource.getSchema());
    }

    @Test
    public void testGetTypeNames() throws IOException {
        String[] names = bcgisDataStore.getTypeNames();
        Assert.assertNotNull(names);
//        System.out.println("typenames: " + names.length);
//        System.out.println("typename[0]: " + names[0]);
    }

    @Test
    public void testGetGeometryDescriptor() throws IOException {
        SimpleFeatureType type = bcgisDataStore.getSchema(bcgisDataStore.getTypeNames()[0]);
        GeometryDescriptor descriptor = type.getGeometryDescriptor();
        System.out.println(descriptor.getType());
    }

    @Test
    public void testGetBounds() throws IOException {
        ContentFeatureSource bcgisFeatureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        FeatureCollection featureCollection = bcgisFeatureSource.getFeatures();
        ReferencedEnvelope env = DataUtilities.bounds(featureCollection);
        System.out.println(env);
    }

    @Test
    public void testGetSchema() throws IOException {
        SimpleFeatureType type = bcgisDataStore.getSchema(bcgisDataStore.getTypeNames()[0]);
        System.out.println(type);
        Assert.assertNotNull(type.getAttributeDescriptors());

//        System.out.println("featureType  name: " + type.getName());
//        System.out.println("featureType count: " + type.getAttributeCount());
//
//        for (AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
//            System.out.print("  " + descriptor.getName());
//            System.out.print(
//                    " (" + descriptor.getMinOccurs() + "," + descriptor.getMaxOccurs() + ",");
//            System.out.print((descriptor.isNillable() ? "nillable" : "manditory") + ")");
//            System.out.print(" type: " + descriptor.getType().getName());
//            System.out.println(" binding: " + descriptor.getType().getBinding().getSimpleName());
//        }
//
//        AttributeDescriptor attributeDescriptor = type.getDescriptor(0);
//        System.out.println("attribute 0    name: " + attributeDescriptor.getName());
//        System.out.println("attribute 0    type: " + attributeDescriptor.getType().toString());
//        System.out.println("attribute 0 binding: " + attributeDescriptor.getType().getBinding());
    }

    @Test
    public void testGetFeatureReaderFromFeatureSource() throws IOException {
        Query query = new Query(bcgisDataStore.getTypeNames()[0]);

        System.out.println("open feature reader");
        FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                bcgisDataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        try {
            int count = 0;
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                if (feature != null) {
                    System.out.println("  " + feature.getID() + " " + feature.getAttribute("geom"));
                    Assert.assertNotNull(feature);
                    count++;
                }
            }
//            System.out.println("close feature reader");
            System.out.println("read in " + count + " features");
        } finally {
            reader.close();
        }
    }

    @Test
    public void testGetFeatureCount() throws IOException {
        ContentFeatureSource bcgisFeatureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        int n = bcgisFeatureSource.getCount(Query.ALL);
        System.out.println(n);
        Assert.assertNotEquals(-1, n);
    }

    @Test
    public void testGetDataStoreByParam() throws IOException {
        Map<String, Serializable> params = new HashMap<>();
        params.put("config", networkFile);
        params.put("chaincodeName", "bincc");
        params.put("functionName", "GetByteArray");
        params.put("recordKey", "Line");
        DataStore store = DataStoreFinder.getDataStore(params);
        Query query = new Query(bcgisDataStore.getTypeNames()[0]);
        FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                bcgisDataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        try {
            int count = 0;
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                if (feature != null) {
                    System.out.println("  " + feature.getID() + " " + feature.getAttribute("geom"));
                    System.out.println(feature);
//                    Assert.assertNotNull(feature);
                    count++;
                }
            }
        } finally {
            reader.close();
        }

        String names[] = store.getTypeNames();
        System.out.println("typenames: " + names.length);
        System.out.println("typename[0]: " + names[0]);
    }

    // test FeatureStore write function

    @Test
    public void testFeatureWrite() throws IOException {
        String typeName = bcgisDataStore.getTypeNames()[0];
        SimpleFeatureType  featureType = bcgisDataStore.getSchema(typeName);
        SimpleFeatureStore featureStore = (SimpleFeatureStore) bcgisDataStore.getFeatureSource(typeName);
        SimpleFeatureStore featureStore1 = (SimpleFeatureStore) bcgisDataStore.getFeatureSource(typeName);
        SimpleFeatureStore featureStore2 = (SimpleFeatureStore) bcgisDataStore.getFeatureSource(typeName);

        Transaction t1 = new DefaultTransaction("t1");
        Transaction t2 = new DefaultTransaction("t2");

        featureStore1.setTransaction(t1);
        featureStore2.setTransaction(t2);

        System.out.println("Step 1");
        System.out.println("------");
        System.out.println("start     auto-commit: " + DataUtilities.fidSet(featureStore.getFeatures()));
        System.out.println("start              t1: " + DataUtilities.fidSet(featureStore1.getFeatures()));
        System.out.println("start              t2: " + DataUtilities.fidSet(featureStore2.getFeatures()));

        // 测试删除 featureStore1 中的数据，在事务 t1 commit 之前，删除操作只会记录在 DataStore 中，commit 之后 才会写入数据库
        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
        Filter filter1 = ff.id(Collections.singleton(ff.featureId("tmpTypeName.1")));
        featureStore1.removeFeatures(filter1);
        System.out.println();
        System.out.println("Step 2 transaction 1 removes feature 'fid1'");
        System.out.println("------");
        System.out.println("t1 remove auto-commit: " + DataUtilities.fidSet(featureStore.getFeatures()));
        System.out.println("t1 remove          t1: " + DataUtilities.fidSet(featureStore1.getFeatures()));
        System.out.println("t1 remove          t2: " + DataUtilities.fidSet(featureStore2.getFeatures()));

//        GeometryFactory geometryFactory = new GeometryFactory();
//        LineString lineString = geometryFactory.createLineString(new Coordinate[]{new Coordinate(10.0, 13.0), new Coordinate(23.0, 26.0)});
//        LineString[] lineStrings = {lineString, lineString};
//        MultiLineString multiLineString = geometryFactory.createMultiLineString(lineStrings);
//        SimpleFeature feature = SimpleFeatureBuilder.build(featureType, new Object[]{multiLineString}, "line1");
//        SimpleFeatureCollection collection = DataUtilities.collection(feature);
//        featureStore2.addFeatures(collection);

//        System.out.println();
//        System.out.println("Step 3 transaction 2 adds a new feature '" + feature.getID() + "'");
//        System.out.println("------");
//        System.out.println(
//                "t2 add    auto-commit: " + DataUtilities.fidSet(featureStore.getFeatures()));
//        System.out.println(
//                "t2 add             t1: " + DataUtilities.fidSet(featureStore1.getFeatures()));
//        System.out.println(
//                "t1 add             t2: " + DataUtilities.fidSet(featureStore2.getFeatures()));

        // 提交事务
//        t1.commit();
//        t2.commit();

//        t1.close();
//        t2.close();
        bcgisDataStore.dispose();
    }

    /**
     * 以JFrame方式显示地图
     */
    public static void main(String[] args) throws IOException {
        String LineKey = "5668c664c852b2b95543b784371f0267136cb4e09b8cb4a284148d2b9f578301";
        //   D             6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4
        //  中国地图       23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8
        //  成都区         5668c664c852b2b95543b784371f0267136cb4e09b8cb4a284148d2b9f578301
        BCGISDataStore bcgisDataStore = new BCGISDataStore(
                new File("\\E:\\DemoRecording\\A_SuperMap\\ATLab-gt-fabric\\src\\test\\resources\\network-config-test.yaml"),
                "bcgiscc",
                "GetRecordByKey",
                LineKey
        );
        SimpleFeatureSource simpleFeatureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);


        simpleFeatureSource.getSchema();
        String typeName = bcgisDataStore.getTypeNames()[0];
        SimpleFeatureType type = bcgisDataStore.getSchema(typeName);

        MapContent map = new MapContent();
        map.setTitle("北京");
        Style style = SLD.createSimpleStyle(type);

        Layer layer = new FeatureLayer(simpleFeatureSource, style);
        map.addLayer(layer);
        JMapFrame.showMap(map);
    }

    /**
     * 通过 bcgis 插件将 shp 文件存入区块链网络
     */
    @Test
    public void testPutDataOnBlockchain() throws IOException, InterruptedException {
        String result = bcgisDataStore.putDataOnBlockchain(shpFile);
        System.out.println(result);
//        Assert.assertTrue(result.contains("successfully"));
    }

    /**
     *  根据 hash 获取数据
     */
    @Test
    public void testGetDataFromChain() {
        Geometry geometry = bcgisDataStore.getRecord();
        System.out.println(geometry.getNumGeometries());
    }

    /**
     *  测试couchDB 的富查询
     *  根据属性值 D 和对应的 hash 值进行查询
     *  return 得到 geometryCollection
     */
    @Test
    public void testGetRecordByAttributes(){
        List<String> stringList = new ArrayList<>();
        String key = "taiwan";
        String hash = "23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8";
        //   D             6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4      属性  D
        //  中国地图        23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8     属性 省市行政区名
        stringList.add(key);
        stringList.add(hash);
        Geometry geometry = bcgisDataStore.getRecordByAttributes(stringList);
        System.out.println(geometry);
        System.out.println(geometry.getNumGeometries());
    }
}