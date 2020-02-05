package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * 主要功能是采用分页的方式将BCGISDataStore解析得到的geometry和属性信息加载到builder中
 */
public class BCGISFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    Logger logger = Logger.getLogger(BCGISFeatureReader.class.toString());

    protected ContentState state;
    protected Geometry geometry;
    protected JSONArray jsonArray;
    protected SimpleFeatureBuilder builder;
    private GeometryFactory geometryFactory;
    private int totalCount;
    private JSONArray jsonArrayReadRange;
    // 记录有多少个geometry
    private int index = 0;
    private BCGISDataStore bcgisDataStore;
    // 记录分页用
    private int page = 0;
    // 得到每一页 geometry 的序号
    private int tmpCount = 0;

    /**
     * 分页解析空间数据和属性（解决内存占用过高的问题）
     * @param contentState
     * @param query
     */
    public BCGISFeatureReader(ContentState contentState, Query query) {
        logger.info("run BCGISFeatureReader");
        this.state = contentState;
        this.builder = new SimpleFeatureBuilder(state.getFeatureType());
        bcgisDataStore = (BCGISDataStore) contentState.getEntry().getDataStore();
        totalCount = (int) bcgisDataStore.getCount().get(0);
        jsonArrayReadRange = JSONArray.parseArray(bcgisDataStore.getCount().get(1).toString());
        // 做判断，因为为分页解析，当把当前页解析完之后就需要解析下一页
        if (index >= (int) jsonArrayReadRange.get(page)) {
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
            // 当  tmpCount 超过一页的时候就需要重新读取数据
            if(tmpCount < geometry.getNumGeometries()){
                Geometry geom = geometry.getGeometryN(tmpCount);
                JSONArray json = (JSONArray)jsonArray.get(tmpCount);
                feature = getFeature(geom, json);
            } else if(jsonArrayReadRange.size() > 2) {
                // 新的一页，需要新的 数据和属性
                page = page + 1;
                tmpCount = 0;
                geometry = null;
                jsonArray = null;
                geometry = bcgisDataStore.getRecord(page);
                if(geometry != null ) { // 小于0表示下一页没有了
                    jsonArray = bcgisDataStore.getProperty(page);
                    Geometry geom = geometry.getGeometryN(tmpCount);
                    JSONArray json = (JSONArray) jsonArray.get(tmpCount);
                    feature = getFeature(geom, json);
                }
            }

        }
        return feature;
    }

    /**
     * 将 geometry 和属性解析到 builder 中
     * @param geometry
     * @param json
     * @return
     */
    private SimpleFeature getFeature(Geometry geometry, JSONArray json) {
        if(geometry == null){
            return null;
        }
        if(json.size() == 0){
            return null;
        }
        index ++;
        tmpCount ++;
//        builder.set("geom", geometry);
        builder.set("geom", geometryFactory.createGeometry(geometry));
        // 直接获取已存入的属性值
        List<AttributeDescriptor> list = builder.getFeatureType().getAttributeDescriptors();
        if( list.size() - json.size() != 1){
            logger.info("警告，请检查数据的属性是否缺失");
        }
        for(int k = 0; k < json.size(); k ++){
            String key = list.get(k + 1).getLocalName(); // 获取 builde 中有那些属性字段
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
