package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
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

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {

        FeatureCollection featureCollection = getFeatures();
        FeatureIterator iterator = featureCollection.features();
        ReferencedEnvelope env = DataUtilities.bounds(iterator);
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

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();

        builder.setName(entry.getName());
        builder.setCRS(DefaultGeographicCRS.WGS84);
        BCGISDataStore bcgisDataStore = getDataStore();
        Geometry geometry = bcgisDataStore.getRecord();
        JSONArray jsonArray = bcgisDataStore.getProperty();

        if (geometry.getNumGeometries() < 1) {
            builder.add("geom", LineString.class);
        } else {
            String geometryType = geometry.getGeometryN(0).getGeometryType().toLowerCase();
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
        // TODO  在 reader/getFeature 中增加属性时，这里需要先将有哪些属性告诉对方，然后再添加
        JSONObject jsonObject = JSONObject.parseObject(jsonArray.get(0).toString());
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
















