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
            int a = index;
            JSONObject jsonObject = JSONObject.parseObject(jsonArray.get(index).toString());
            feature = getFeature(geom, jsonObject);
        }
        return feature;
    }

    private SimpleFeature getFeature(Geometry geometry, JSONObject jsonObject) {
        if(geometry == null){
            return null;
        }
        index ++;
//        builder.set("geom", geometry);
        builder.set("geom", geometryFactory.createGeometry(geometry));

        Set<String> keys =  jsonObject.keySet();
        for(String key : keys){
            String value = jsonObject.get(key).toString();
            builder.set(key, value);
        }
        return builder.buildFeature(state.getEntry().getTypeName() + "." + index);
    }

    @Override
    public boolean hasNext() {
        if (index < geometry.getNumGeometries()){
            return true;
        } else if (geometry == null){
            return  false;
        } else {
            JSONObject jsonObject = new JSONObject();
            next = getFeature(geometry, jsonObject);
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
