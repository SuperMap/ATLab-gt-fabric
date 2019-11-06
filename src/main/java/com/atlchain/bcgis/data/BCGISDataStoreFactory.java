package com.atlchain.bcgis.data;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataUtilities;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

public class BCGISDataStoreFactory implements DataStoreFactorySpi {

    Logger logger = Logger.getLogger(BCGISDataStoreFactory.class.toString());

    public BCGISDataStoreFactory() {}

    @Override
    public Map<RenderingHints.Key, ?> getImplementationHints() {

        return Collections.emptyMap();
    }

    @Override
    public String getDisplayName() { return "BCGIS"; }

    @Override
    public String getDescription() { return "WKB binary file"; }

    @Override
    public boolean isAvailable() { return true; }

    public static final Param NETWORK_CONFIG_PARAM  = new Param("config", File.class, "network config file");
    public static final Param CC_NAME_PARAM         = new Param("chaincodeName", String.class, "chaincode name");
    public static final Param FUNCTION_NAME_PARAM   = new Param("functionName", String.class, "function name");
    public static final Param KEY_PARAM             = new Param("recordKey", String.class, "record key");
    public static final Param NAMESPACE             = new Param("namespace", String.class, "Namespace URI", false);
    @Override
    public Param[] getParametersInfo() {
        return new Param[] {
                NAMESPACE,
                NETWORK_CONFIG_PARAM ,
                CC_NAME_PARAM,
                FUNCTION_NAME_PARAM,
                KEY_PARAM
        };
    }

    @Override
    public boolean canProcess(Map<String, Serializable> params) {
        return DataUtilities.canProcess(params, getParametersInfo());
    }

    @Override
    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {

        File file = (File)NETWORK_CONFIG_PARAM.lookUp(params);
        File networkConfigFile_InputValue = null;

        if (file.getPath().startsWith("file:")) {
            String path = file.getPath();
            path = path.replace("\\",File.separator);
            networkConfigFile_InputValue = new File(new URL(path).getPath());
        } else {
            networkConfigFile_InputValue = file;
        }

        String string_temp = null;
        File file_temp = null;
        File networkConfigFile = null;

        if(networkConfigFile_InputValue.exists()){
            networkConfigFile = networkConfigFile_InputValue;
        }else{
            string_temp = "data_dir" + File.separator + networkConfigFile_InputValue.getPath();
            file_temp = new File(string_temp);
            string_temp = file_temp.toURL().toString().replace("\\",File.separator);
            networkConfigFile = new File(new URL(string_temp).getPath());
        }

        String chaincodeName = (String)CC_NAME_PARAM.lookUp(params);
        String functionName = (String)FUNCTION_NAME_PARAM.lookUp(params);
        String key = (String)KEY_PARAM.lookUp(params);
        String uri = (String) NAMESPACE.lookUp(params);

        BCGISDataStore bcgisDataStore = new BCGISDataStore(
                networkConfigFile,
                chaincodeName,
                functionName,
                key
        );

        if (uri != null) {
            bcgisDataStore.setNamespaceURI(uri);
        }

        return bcgisDataStore;
    }

    @Override
    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        File networkConfigFile = new File(((File)NETWORK_CONFIG_PARAM.lookUp(params)).getPath());

        String chaincodeName = (String)CC_NAME_PARAM.lookUp(params);
        String functionName = (String)FUNCTION_NAME_PARAM.lookUp(params);
        String key = (String)KEY_PARAM.lookUp(params);

        BCGISDataStore bcgisDataStore = new BCGISDataStore(
                networkConfigFile,
                chaincodeName,
                functionName,
                key
        );
        return bcgisDataStore;
    }

}
