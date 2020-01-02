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
     * 分页解析空间数据和属性（考虑内存占用的问题）
     * @param contentState
     * @param query
     */
    public BCGISFeatureReader(ContentState contentState, Query query) {
        this.state = contentState;
        builder = new SimpleFeatureBuilder(state.getFeatureType());
        bcgisDataStore = (BCGISDataStore)contentState.getEntry().getDataStore();
        totalCount = (int)bcgisDataStore.getCount().get(0);
        jsonArrayReadRange = JSONArray.parseArray(bcgisDataStore.getCount().get(1).toString());
        // 做一个判断 如何获取新值 大于等于小于 说明这一页读完，需解析下一页
        if(index >= (int)jsonArrayReadRange.get(page)){
            geometry = bcgisDataStore.getRecord(page);
            jsonArray = bcgisDataStore.getProperty(page);
            tmpCount = 0;
        }
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
            // 当  tmpCount 超过一页的时候就需要重新读取数据了
            if(tmpCount < geometry.getNumGeometries()){
                Geometry geom = geometry.getGeometryN(tmpCount);
                JSONArray json = (JSONArray)jsonArray.get(tmpCount);
                feature = getFeature(geom, json);
            } else {
                // 新的一页，需要新的 数据和属性
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

    private SimpleFeature getFeature(Geometry geometry, JSONArray json) {
        if(geometry == null){
            return null;
        }
        index ++;
        tmpCount ++;
//        builder.set("geom", geometry);
        builder.set("geom", geometryFactory.createGeometry(geometry));
        // 下述方法可直接获取有哪些属性值，到时直接存入即可，不用JSON字符串，节省一半空间
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
