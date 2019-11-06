package com.atlchain.bcgis.data;

import org.geotools.data.*;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.data.store.ContentState;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import java.io.IOException;
import java.util.logging.Logger;

public class BCGISFeatureStore extends ContentFeatureStore {
    Logger logger = Logger.getLogger(BCGISFeatureStore.class.toString());

    public BCGISFeatureStore(ContentEntry entry, Query query) {
        super(entry, query);
    }

    BCGISFeatureSource delegate = new BCGISFeatureSource(entry, query){
        @Override
        public void setTransaction(Transaction transaction) {
            super.setTransaction(transaction);
            BCGISFeatureStore.this.setTransaction(transaction);
        }
    };


    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(
            Query query, int flags) {
        return new BCGISFeatureWriter(getState(), query);
    }

    @Override
    public void setTransaction(Transaction transaction) {
        super.setTransaction(transaction);
        if (delegate.getTransaction() != transaction) {
            delegate.setTransaction(transaction);
        }
    }

    @Override
    public BCGISDataStore getDataStore() {

        return delegate.getDataStore();
    }

    @Override
    public ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return delegate.getBoundsInternal(query);
    }

    @Override
    public int getCountInternal(Query query) {

        return delegate.getCountInternal(query);
    }

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) {
        return delegate.getReaderInternal(query);
    }

    @Override
    public SimpleFeatureType buildFeatureType() {
        return delegate.buildFeatureType();
    }

    @Override
    public boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException {
        return delegate.handleVisitor(query, visitor);
    }

    @Override
    public ContentEntry getEntry() {
        return delegate.getEntry();
    }

    @Override
    public Transaction getTransaction() {
        return delegate.getTransaction();
    }

    @Override
    public ContentState getState() {
        return delegate.getState();
    }

    @Override
    public ResourceInfo getInfo() {
        return delegate.getInfo();
    }

    @Override
    public Name getName() {
        return delegate.getName();
    }

    @Override
    public QueryCapabilities getQueryCapabilities() {
        return delegate.getQueryCapabilities();
    }

}
