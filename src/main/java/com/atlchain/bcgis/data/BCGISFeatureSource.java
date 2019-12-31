package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.shapefile.shp.ShapefileHeader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.FeatureCollection;
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
import java.util.Set;
import java.util.logging.Logger;

public class BCGISFeatureSource extends ContentFeatureSource {

    Logger logger = Logger.getLogger(BCGISFeatureSource.class.toString());

    public BCGISFeatureSource(ContentEntry entry, Query query) {

        super(entry, query);
    }

    public BCGISDataStore getDataStore() {

        return (BCGISDataStore) super.getDataStore();
    }

    // TODO 这里的范围计算会计算两次读数据  写数据  看如何改进
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {

//        FeatureCollection featureCollection = getFeatures();
//        FeatureIterator iterator = featureCollection.features();
//        // TODO 这种计算范围的方法需要在改进下
//        ReferencedEnvelope env = DataUtilities.bounds(featureCollection);
//        logger.info("计算范围" + env);
//        iterator.close();
//        return env;
        ShapefileHeader header = new ShapefileHeader();



        BCGISDataStore bcgisDataStore = getDataStore();
        SimpleFeatureSource bcgisFeatureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        SimpleFeatureCollection featureCollection = bcgisFeatureSource.getFeatures();
        ReferencedEnvelope env = featureCollection.getBounds();

        return env;

    }

    @Override
    protected int getCountInternal(Query query) {
        if(query.getFilter() == Filter.INCLUDE){
            Geometry geometry = getDataStore().getRecord();
            int count = geometry.getNumGeometries();
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
        // TODO 下面计算的 geometry 的目的只是为了获取 geom 完全用不着将数据全部读取一次，需要修改
        // 两种方式  第一种：直接从属性数据中获取
        //          第二种：读取整体信息解析得到（暂时改为这一种)
//        Geometry geometry = bcgisDataStore.getRecord();
        String geometryType = bcgisDataStore.getGeotype().toLowerCase();
        JSONObject jsonObject = bcgisDataStore.getPropertynName(0);

//        if (geometry.getNumGeometries() < 1) {
        if(geometryType == null){
            builder.add("geom", LineString.class);
        } else {
//            String geometryType = geometry.getGeometryN(0).getGeometryType().toLowerCase();
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
        }
        logger.info("添加属性名称环节");
        Set<String> keys =  jsonObject.keySet();
        for(String key : keys){
            builder.add(key, String.class);
        }
        final SimpleFeatureType SCHEMA = builder.buildFeatureType();
        return SCHEMA;
    }

    @Override
    protected boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException{
        return super.handleVisitor(query,visitor);
        // WARNING: Please note this method is in BCGISeatureSource!
    }
}
















