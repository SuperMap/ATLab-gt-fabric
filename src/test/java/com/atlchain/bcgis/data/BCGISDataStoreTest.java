package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureCollection;
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
    private String shpURL = this.getClass().getResource("/beijing/R.shp").getFile();
    // 点    /beijing/P.shp
    // 线    /BL/BL.shp       /beijing/R.shp
    // 面    /D/D.shp        /Country_R/Country_R.shp    /Province/Province_R.shp
    private File shpFile = new File(shpURL);
    private String chaincodeName = "bcgiscc";
    //   D               6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4
    //  Province         23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8
    //  BL              d7e94bf0c86c94579e8b564d2dea995ed3746108f98f003fb555bcd41831f885
    //  P               b6a5833aba1f3a73e9d721a6df15defd00b17a3722491bb33b7700d37f288d5b
    // Country_R.shp    8407bd3cd93d156e026b3cccba12035ef10b85b1ba1db31590296a153af7f3db
    // beijing/R.shp    278934ff40e23d4a054144b495df7ca5eb0f764aa02d44f0cf02b8921539d8b1
    private String functionName = "GetRecordByKey";
    private String recordKey = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4";

    private File networkFile = new File(this.getClass().getResource("/network-config-test.yaml").getPath());
   // 存数据时 （recordKey = "null"）
    private BCGISDataStore bcgisDataStore = new BCGISDataStore(
            networkFile,
            shpFile,
            chaincodeName,
            functionName,
            "null"
//           recordKey
    );

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    // test FeatureStore read function
    @Test
    public void testFeatureSource() throws IOException {
        ContentFeatureSource featureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        System.out.println(featureSource.getSchema());
    }

    /**
     * 获取名称（完成）
     * @throws IOException
     */
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
        int count = type.getAttributeCount(); // 根据BCGISFeatureSource/SimpleFeatureType返回SCHEMA得到属性
        GeometryDescriptor descriptor = type.getGeometryDescriptor();
        System.out.println(descriptor.getType());
    }

    @Test
    public void testGetBounds() throws IOException {
        SimpleFeatureSource bcgisFeatureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        SimpleFeatureCollection featureCollection = bcgisFeatureSource.getFeatures();
        ReferencedEnvelope env = DataUtilities.bounds(featureCollection);
        ReferencedEnvelope env1 = featureCollection.getBounds();
        System.out.println(env);
        System.out.println(env1);
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
     * 通过 bcgis 插件将 shp 文件存入区块链网络
     */
    @Test
    public void testPutDataOnBlockchain() throws IOException, InterruptedException {
        String result = bcgisDataStore.putDataOnBlockchain(shpFile);
        System.out.println(result);
//        Assert.assertTrue(result.contains("successfully"));
    }

    /**
     * 2019.12.17
     * 通过 bcgis 插件将 shp 文件 以 proto 格式存入区块链网络
     */
    @Test
    public void testPutDataOnBlockchainByProto() throws IOException, InterruptedException {
        String result = bcgisDataStore.putDataOnBlockchainByProto();
        System.out.println(result);
//        Assert.assertTrue(result.contains("successfully"));
    }

    /**
     *  根据 hash 获取数据
     */
    @Test
    public void testGetDataFromChain() {

//        Geometry geometry = bcgisDataStore.getRecord();
//        System.out.println(geometry.getNumGeometries());
//        JSONArray jsonArray = bcgisDataStore.getProperty();
//        for(Object o : jsonArray){
//            JSONObject json = JSONObject.parseObject(o.toString());
////            System.out.println(json);
//        }
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
        Geometry geometry = bcgisDataStore.queryAttributes(stringList);
        System.out.println(geometry);
        System.out.println(geometry.getNumGeometries());
    }


    /**
     * 基于 proto 格式实现属性查询------------------>根据 key 对应的多个 value进行查询
     *    ------>前提是前端在展示的时候，让用户进行属性查询时先确定查询的属性是什么？然后在填写查询的东西------->设计一个类似于搜索的东西
     *
     *    前端展示需要一个下拉列表框，展示现在具体有哪些属性，然后用户选择查询那个属性------>输入值，可输入多个
     *                                                  可选择多个属性进行综合查询，然后输入属性查询
     *    到时前端展示的时候只显示几个固定的查询框即可，后期在做上面的想法
     */
    @Test
    public void testQueryAttributes(){

        JSONObject json = new JSONObject();
        String key1 = "AdminCode";
        String key2 = "Kind";
        JSONArray value1 = new JSONArray();
        JSONArray value2 = new JSONArray();
        value1.add("130824");  // 唯一值
        value1.add("110228");
        value1.add("110116");
        value2.add("0137");    // 全部都一样
        json.put(key1, value1);
        json.put(key2, value2);
        Geometry geometry = bcgisDataStore.queryAttributesByProto(json);
        System.out.println(geometry.getNumGeometries());
    }

    @Test
    public void testGetSinglePropFromChainCode(){
        // 依靠hashID获取单个属性
        JSONObject jsonObject = bcgisDataStore.getSinglePropFromChainCode(0);
        System.out.println(jsonObject);
        // 依靠hashID获取全部属性
        JSONArray jsonArray = bcgisDataStore.getProperty(0);
        for(Object o : jsonArray){
            System.out.println(o);
        }
    }
}