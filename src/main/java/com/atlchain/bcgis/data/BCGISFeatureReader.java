package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.protoBuf.GeoDataOuterClass;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

public class BCGISFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    Logger logger = Logger.getLogger(BCGISFeatureReader.class.toString());

    protected ContentState state;
    protected Geometry geometry;
    protected JSONArray jsonArray;
    protected SimpleFeatureBuilder builder;
    private GeometryFactory geometryFactory;
    private int totalCount;
    private JSONArray jsonArrayReadRange;
    private int index = 0;
    private BCGISDataStore bcgisDataStore;

    // 记录分页用
    private int page = 0;
    // 得到每一组 geometry 的序号
    private int tmpCount = 0;

    /**
     * 属性的用处  在这里直接调用，然后解析即可，不用全部调用
     * @param contentState
     * @param query
     */
    // 属性的需要其实就这里（需要全部的属性，我觉得在这里直接读取即可，不用提前解析）
    // 和 之前 FeatureSource里面（只需要有哪些属性即可）
    public BCGISFeatureReader(ContentState contentState, Query query) {
        logger.info("====================================================================");
        this.state = contentState;
        bcgisDataStore = (BCGISDataStore)contentState.getEntry().getDataStore();
//        geometry = bcgisDataStore.getRecord();
//        jsonArray = bcgisDataStore.getProperty();
        // TODO 数据的获取应根据应分页加载
        totalCount = (int)bcgisDataStore.getCount().get(0);
        jsonArrayReadRange = JSONArray.parseArray(bcgisDataStore.getCount().get(1).toString());
        // 做一个判断 如何获取新值
        // 小于 说明这一页还没读完 就继续用
        if(index < (int)jsonArrayReadRange.get(page)){
//            geometry = bcgisDataStore.getRecord(page);
//            jsonArray = bcgisDataStore.getProperty(page);
        } else {
            // 重新开启新的一页
            geometry = bcgisDataStore.getRecord(page);
            jsonArray = bcgisDataStore.getProperty(page);
            tmpCount = 0;
        }
        builder = new SimpleFeatureBuilder(state.getFeatureType());
        geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return (SimpleFeatureType) state.getFeatureType();
    }

    private SimpleFeature next;
    @Override
    public SimpleFeature next() throws IllegalArgumentException, NoSuchElementException {
        SimpleFeature feature = null;
        if(next != null){
            feature = next;
            next = null;
        }else{
            // TODO 根据索引来读取数据 ，将bcgisDataStore 作为全局变量
            // 当  tmpCount 超过一页的时候就需要重新读取数据了  怎么触发这个条件
            if(tmpCount < geometry.getNumGeometries()){
                //            Geometry geom = geometry.getGeometryN(index);
                Geometry geom = geometry.getGeometryN(tmpCount);
//            JSONObject json = (JSONObject) jsonArray.get(index);
//            JSONArray json = (JSONArray) jsonArray.get(index);
                JSONArray json = (JSONArray)jsonArray.get(tmpCount);
                feature = getFeature(geom, json);
            } else {
                // 需要新的 数据和属性
                page = page + 1;
                tmpCount = 0;
                geometry = null;
                jsonArray = null;
                geometry = bcgisDataStore.getRecord(page);
                jsonArray = bcgisDataStore.getProperty(page);
                Geometry geom = geometry.getGeometryN(tmpCount);
                JSONArray json = (JSONArray)jsonArray.get(tmpCount);
                feature = getFeature(geom, json);
            }

        }
        return feature;
    }

    // TODO 想节流这里就只根据 ID 获取信息即可看行不行 （每次从区块链读取）
    private SimpleFeature getFeature(Geometry geometry, JSONArray json) {
        if(geometry == null){
            return null;
        }
        index ++;
        tmpCount ++;
//        builder.set("geom", geometry);
        builder.set("geom", geometryFactory.createGeometry(geometry));

//        Set<String> keys =  jsonObject.keySet();
//        for(String key : keys){
//            String value = jsonObject.get(key).toString();
//            builder.set(key, value);
//        }
        // TODO 下述方法可直接获取有哪些属性值，到时直接存入即可，不用JSON字符串，节省一半空间
        List<AttributeDescriptor> list = builder.getFeatureType().getAttributeDescriptors();
//        System.out.println(list.get(0).getLocalName()); // 获取 builde 中有那些属性字段
        for(int k = 0; k < json.size(); k ++){
            String key = list.get(k + 1).getLocalName();
            String value = json.get(k).toString();
            builder.set(key, value);
        }
        return builder.buildFeature(state.getEntry().getTypeName() + "." + index);
    }

    @Override
    public boolean hasNext() {
//        if (index < geometry.getNumGeometries()){
        if(index < totalCount){
            return true;
        } else if (geometry == null){
            return  false;
        } else {
            JSONObject jsonObject = new JSONObject();
            JSONArray json = new JSONArray();
            next = getFeature(geometry, json);
            return false;
        }
    }

    @Override
    public void close() {
        if(geometry != null){
            geometry = null;
            jsonArray = null;
        }
        builder = null;
        geometryFactory = null;
        next = null;
    }
}
