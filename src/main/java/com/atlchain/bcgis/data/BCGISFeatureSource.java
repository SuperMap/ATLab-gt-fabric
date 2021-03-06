package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.*;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class BCGISFeatureSource extends ContentFeatureSource {

    Logger logger = Logger.getLogger(BCGISFeatureSource.class.toString());

    public BCGISFeatureSource(ContentEntry entry, Query query) {

        super(entry, query);
    }

    /**
     * 构造 getDataStore 得到 BCGISDataStore
     * @return
     */
    public BCGISDataStore getDataStore() {

        return (BCGISDataStore) super.getDataStore();
    }

    /**
     * 计算展示范围
     * @param query
     * @return
     * @throws IOException
     */
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {

        SimpleFeatureSource bcgisFeatureSource = getDataStore().getFeatureSource(getDataStore().getTypeNames()[0]);
        SimpleFeatureCollection featureCollection = bcgisFeatureSource.getFeatures();
        FeatureIterator iterator = featureCollection.features();
        ReferencedEnvelope env = DataUtilities.bounds(featureCollection);
        iterator.close();
        logger.info("计算范围为：" + env);
        return env;
    }

    @Override
    protected int getCountInternal(Query query) {
        if(query.getFilter() == Filter.INCLUDE){
            int count = (int)getDataStore().getCount().get(0);
            return count;
        }
        return -1;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
        return new BCGISFeatureReader(getState(), query);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() {

        logger.info("buildFeatureType");
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();

        builder.setName(entry.getName());
        builder.setCRS(DefaultGeographicCRS.WGS84);
        BCGISDataStore bcgisDataStore = getDataStore();
        List<Object> list = bcgisDataStore.getPropertynName();
        String geometryType = list.get(0).toString().toLowerCase();
        JSONArray jsonArray = (JSONArray) list.get(1);

        // 判断geometry的类型，注意多点线面在前，单点线面在后
        if(geometryType.equals("multipoint")) {
            builder.add("geom", MultiPoint.class);
        }else if(geometryType.equals("point")) {
            builder.add("geom", Point.class);
        }else if(geometryType.equals("multilinestring")) {
            builder.add("geom", MultiLineString.class);
        }else if(geometryType.equals("linestring")){
            builder.add("geom",LineString.class);
        }else if (geometryType.contains("multipolygon")) {
            builder.add("geom", MultiPolygon.class);
        }else if(geometryType.contains("polygon")) {
            builder.add("geom", Polygon.class);
        }
        for(int i = 0; i < jsonArray.size(); i++){
            builder.add(jsonArray.get(i).toString(), String.class);
        }
        logger.info("完成加载属性键值 ");
        final SimpleFeatureType SCHEMA = builder.buildFeatureType();
        return SCHEMA;
    }

    @Override
    protected boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException{
        return super.handleVisitor(query,visitor);
    }
}
















