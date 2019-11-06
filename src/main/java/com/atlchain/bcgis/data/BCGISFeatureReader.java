package com.atlchain.bcgis.data;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.NoSuchElementException;
import java.util.logging.Logger;

public class BCGISFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    Logger logger = Logger.getLogger(BCGISFeatureReader.class.toString());

    protected ContentState state;

    protected Geometry geometry;

    protected SimpleFeatureBuilder builder;

    private GeometryFactory geometryFactory;

    private int index = 0;

    public BCGISFeatureReader(ContentState contentState, Query query) {
        this.state = contentState;
        BCGISDataStore bcgisDataStore = (BCGISDataStore)contentState.getEntry().getDataStore();
        geometry = bcgisDataStore.getRecord();
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
            feature = getFeature(geom);
        }
        return feature;
    }

    private SimpleFeature getFeature(Geometry geometry) {
        if(geometry == null){
            return null;
        }
        index ++;
//        builder.set("geom", geometry);
        builder.set("geom",geometryFactory.createGeometry(geometry));
        return builder.buildFeature(state.getEntry().getTypeName() + "." + index);
    }

    @Override
    public boolean hasNext() {
        if (index < geometry.getNumGeometries()){
            return true;
        } else if (geometry == null){
            return  false;
        } else {
            next = getFeature(geometry);
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
