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

    private int index = 0;

    /**
     * 属性的用处  在这里直接调用，然后解析即可，不用全部调用
     * @param contentState
     * @param query
     */
    // 属性的需要其实就这里（需要全部的属性，我觉得在这里直接读取即可，不用提前解析）
    // 和 之前 FeatureSource里面（只需要有哪些属性即可）
    public BCGISFeatureReader(ContentState contentState, Query query) {
        this.state = contentState;
        BCGISDataStore bcgisDataStore = (BCGISDataStore)contentState.getEntry().getDataStore();
        geometry = bcgisDataStore.getRecord();
        jsonArray = bcgisDataStore.getProperty();
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
        SimpleFeature feature;
        if(next != null){
            feature = next;
            next = null;
        }else{
            Geometry geom = geometry.getGeometryN(index);
//            JSONObject jsonObject = JSONObject.parseObject(jsonArray.get(index).toString());
//            feature = getFeature(geom, jsonObject);
            JSONArray json = (JSONArray) jsonArray.get(index);
            feature = getFeature(geom, json);
        }
        return feature;
    }

    // TODO 想节流这里就只根据 ID 获取信息即可看行不行 （每次从区块链读取）
    private SimpleFeature getFeature(Geometry geometry, JSONArray jsonArray) {
        if(geometry == null){
            return null;
        }
        index ++;
//        builder.set("geom", geometry);
        builder.set("geom", geometryFactory.createGeometry(geometry));

//        System.out.println(builder.getFeatureType().getTypes());
        List<AttributeDescriptor> list = builder.getFeatureType().getAttributeDescriptors();

        // TODO 下述方法可直接获取有哪些属性值，到时直接存入即可，不用JSON字符串，节省一般的空间
//        System.out.println(list.get(0).getLocalName()); // 获取 builde 中有那些属性字段

        for(int k = 0; k < jsonArray.size(); k ++){
            String key = list.get(k + 1).getLocalName();
            String value = jsonArray.get(k).toString();
            builder.set(key, value);
        }
//        Set<String> keys =  jsonObject.keySet();
//        for(String key : keys){
//            String value = jsonObject.get(key).toString();
//            builder.set(key, value);
//        }
        return builder.buildFeature(state.getEntry().getTypeName() + "." + index);
    }

    @Override
    public boolean hasNext() {
        if (index < geometry.getNumGeometries()){
            return true;
        } else if (geometry == null){
            jsonArray = null;
            return  false;
        } else {
            JSONArray jsonArray = new JSONArray();
            next = getFeature(geometry, jsonArray);
            return false;
        }
    }

    @Override
    public void close() {
        if(geometry != null){
            geometry = null;
        }
        builder = null;
        geometryFactory = null;
        next = null;
    }
}
