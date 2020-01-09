package com.atlchain.bcgis.data;

import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureWriter;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.IllegalAttributeException;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

public class BCGISFeatureWriter implements SimpleFeatureWriter {

    Logger logger = Logger.getLogger(BCGISFeatureWriter.class.toString());

    private ContentState state;

    BCGISFeatureReader delegate;

    private int nextRow = 0;

    private boolean appending = false;

    private SimpleFeature currentFeature;

    private ArrayList<Geometry> geometryArrayList = new ArrayList<>();

    public BCGISFeatureWriter(ContentState state, Query query) {
        logger.info("writer");
        this.state = state;
        this.delegate = new BCGISFeatureReader(state, query);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return state.getFeatureType();
    }

    @Override
    public boolean hasNext() {
        if(this.appending){
            return false;// reader has no more contents
        }
        return delegate.hasNext();
    }

    @Override
    public SimpleFeature next() throws IOException {
        if(this.currentFeature != null){
            this.write();// the previous one was not written, so do it now.
        }
        try{
            if(!appending){
                if(delegate.geometry != null && delegate.hasNext()){
                    this.currentFeature = delegate.next();
                    return  this.currentFeature;
                }else{
                    this.appending = true;
                }
            }
            SimpleFeatureType featureType = state.getFeatureType();
            String fid = featureType.getTypeName() + "." + nextRow;
            // defaultValues(SimpleFeatureType featureType) Produce a set of default values for the provided FeatureType
            Object values[] = DataUtilities.defaultValues(featureType);

            this.currentFeature = SimpleFeatureBuilder.build(featureType, values,fid);
            return  this.currentFeature;
        }catch (IllegalAttributeException invalid){
            throw new IOException("Unable to create feature :" + invalid.getMessage(), invalid);
        }
    }

    @Override
    public void remove() {
        currentFeature = null;

    }

    @Override
    public void write() {
        if(this.currentFeature == null){
            return;
        }
        for(Property property:currentFeature.getProperties()){
            Object value = property.getValue();
            if(value == null){
                return;
            }else if(value instanceof Geometry){
                Geometry geometry = (Geometry)value;
                geometryArrayList.add(geometry);
            }
        }
        nextRow++;
        this.currentFeature = null ;// indicate that it has been written
    }

    @Override
    public void close() throws IOException {
        if (geometryArrayList != null) {
            this.write();
        }
        while (hasNext()) {
            next();
            write();
        }
        if(delegate != null){
            this.delegate.close();
            this.delegate = null;
        }
    }
}























