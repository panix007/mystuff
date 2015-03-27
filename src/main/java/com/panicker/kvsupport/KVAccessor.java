package com.panicker.kvsupport;

import oracle.kv.*;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.SpecificAvroBinding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.SpecificAvroBinding;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.StringUtils;

public class KVAccessor implements InitializingBean, DisposableBean {

    private static Log logger = LogFactory.getLog(KVAccessor.class);

    private KVStore store;
    private String storeName;
    private String hostName;
    private String hostPort;

    private AvroCatalog catalog;
    private SpecificAvroBinding<SpecificRecord> specificBinding;

    private String[] hostlist = null;

    @Override
    public void afterPropertiesSet() throws Exception {
        try{
            // Connect to store during initialization
            // Log the error for failure but should not stop application to load correctly
            connectToStore();
        } catch ( IOException ioe ) {
            logger.error("IOException: Can't connect to NoSQL KVStore. NoSQL should be running when application is started", ioe);
        } catch ( Exception e ) {
            logger.error("Exception: Can't connect to NoSQL KVStore. NoSQL should be running when applications is started", e);
        }
    }

    public void connectToStore() throws IOException{

        // default to stated single node if no override present
        if( hostlist == null ){
            hostlist =  new String[] {  hostName+ ":" + hostPort };
        }
        logger.info("Connecting to KVStore:: storeName:: " + storeName + " hostName:: " + hostName + " hostPort:: " + hostPort);

    	/* Open the KVStore. */
        store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostlist));

        //Create SpecificBindings for Member schemas
        catalog = store.getAvroCatalog();
        specificBinding = catalog.getSpecificMultiBinding();

        logger.info("Connected to KVStore");

    }

    private void makeSureStoreIsAvailable() {
        try {
            // Make sure that the handle to NoSQL store is available when required
            // it is possible that it was not running when the application was started
            // If not available log error
            if (store == null) {
                synchronized (KVAccessor.class) {
                    if(store == null) {
                        connectToStore();
                    }
                }
            }
        } catch ( IOException ioe ) {
            logger.error("IOException: SERIOUS problem - Can't connect to NoSQL KVStore. NoSQL should be running when it is needed", ioe);
        }
    }

    public boolean write(Key key, byte[] value) {

        return write(key, oracle.kv.Value.createValue(value));
    }

    public boolean write(String strMajorKey, byte[] value) {
        return write(strMajorKey, null, value);
    }

    public boolean write(String strMajorKey, String strMinorKey, byte[] value) {
        boolean writeStatus = false;

        if(!StringUtils.isEmpty(strMajorKey)) {
            writeStatus = write(assembleKey(strMajorKey, strMinorKey), value);
        }

        return writeStatus;
    }

    public boolean write(List<String> majorComponents, List<String> minorComponents, byte[] value) {

        return write(assembleKey(majorComponents, minorComponents), value);
    }

    public boolean write(List<String> majorComponents, List<String> minorComponents, SpecificRecord sr) {

        return write(assembleKey(majorComponents, minorComponents), specificBinding.toValue(sr));
    }

    public boolean write(Key key, Value value) {
        boolean writeStatus = false;

        makeSureStoreIsAvailable();

        if(key != null) {
            Version version = store.put(key, value);
            if(version != null) {
                writeStatus = true;
            }
        }

        return writeStatus;
    }

    public byte[] readValue(String strMajorKey) {
        if(!StringUtils.isEmpty(strMajorKey)) {
            return readValue(strMajorKey, null);
        }

        return null;
    }

    public byte[] readValue(String strMajorKey, String strMinorKey) {

        return readValue(assembleKey(strMajorKey, strMinorKey));
    }

    public byte[] readValue(List<String> majorComponents, List<String> minorComponents) {

        return readValue(assembleKey(majorComponents, minorComponents));
    }

    public byte[] readValue(Key key) {
        byte[] value = null;

        makeSureStoreIsAvailable();

        if(key != null) {
            ValueVersion valueVersion = store.get(key);

            logger.debug("valueVersion::"+valueVersion);

            if(valueVersion != null && valueVersion.getValue() != null) {
                value = valueVersion.getValue().getValue();
            }
        }

        return value;
    }

    public SpecificRecord readSpecificRecord(Key key) {
        SpecificRecord record = null;
        makeSureStoreIsAvailable();

        if(key != null) {
            ValueVersion valueVersion = store.get(key);

            if(valueVersion != null
                    && valueVersion.getValue() != null
                    && Value.Format.AVRO.equals(valueVersion.getValue().getFormat())) {

                record = specificBinding.toObject(valueVersion.getValue());
            }
        }

        return record;
    }

    public SpecificRecord readSpecificRecord(String strMajorKey) {

        return readSpecificRecord(strMajorKey, null);
    }

    public SpecificRecord readSpecificRecord(String strMajorKey, String strMinorKey) {

        return readSpecificRecord(assembleKey(strMajorKey, strMinorKey));
    }

    public SpecificRecord readSpecificRecord(List<String> majorComponents, List<String> minorComponents) {

        return readSpecificRecord(assembleKey(majorComponents, minorComponents));
    }

    public boolean delete(List<String> majorComponents, List<String> minorComponents) {

        return delete(assembleKey(majorComponents, minorComponents));
    }

    public boolean delete(String strMajorKey, String strMinorKey) {

        return delete(assembleKey(strMajorKey, strMinorKey));
    }

    public boolean delete(String strMajorKey) {

        return delete(assembleKey(strMajorKey, null));
    }

    public boolean delete(Key key) {
        boolean delStatus = false;

        makeSureStoreIsAvailable();

        if(key != null) {
            delStatus = store.delete(key);
        }

        return delStatus;
    }

    public Key assembleKey(String strMajorKey) {
        return assembleKey(strMajorKey, null);
    }

    public Key assembleKey(String strMajorKey, String strMinorKey) {
        List<String> majorComponents = null;
        List<String> minorComponents = null;

        if(!StringUtils.isEmpty(strMajorKey)) {
            majorComponents = new ArrayList<String>();
            majorComponents.add(strMajorKey);

            if(!StringUtils.isEmpty(strMinorKey)) {
                minorComponents = new ArrayList<String>();
                minorComponents.add(strMinorKey);
            }
        }
        return assembleKey(majorComponents, minorComponents);
    }

    public Key assembleKey(List<String> majorComponents, List<String> minorComponents) {
        Key myKey = null;

        if(majorComponents != null && !majorComponents.isEmpty()) {
            if(minorComponents != null && !minorComponents.isEmpty()) {
                myKey = Key.createKey(majorComponents, minorComponents);
            } else {
                myKey = Key.createKey(majorComponents);
            }
        }

        return myKey;
    }

    public SortedSet<Key> getAllKeysForMajorPath(List<String> majorComponents) {
        SortedSet<Key> keys = null;

        makeSureStoreIsAvailable();

        if(majorComponents != null && !majorComponents.isEmpty()) {
            keys = store.multiGetKeys(assembleKey(majorComponents, null), null, null);
        }

        return keys;
    }

    public Iterator<Key> getAllKeysForPartialMajorPath(List<String> majorComponents) {
        Iterator<Key> keysItr = null;

        makeSureStoreIsAvailable();

        if(majorComponents != null && !majorComponents.isEmpty()) {
            keysItr = store.storeKeysIterator(Direction.UNORDERED, 0, assembleKey(majorComponents, null), null, null);
        }

        return keysItr;
    }

    public Iterator<Key> getAllKeysForKeyRange(List<String> majorComponents, String start, boolean startInclusive, String end, boolean endInclusive) {
        Iterator<Key> keysItr = null;

        makeSureStoreIsAvailable();

        if(majorComponents != null && !majorComponents.isEmpty()) {

            if(StringUtils.isEmpty(start) || StringUtils.isEmpty(end)) {
                keysItr = store.storeKeysIterator(Direction.UNORDERED, 0, assembleKey(majorComponents, null), null, null);
            } else {

                KeyRange keyRange = new KeyRange(start, startInclusive, end, endInclusive);
                keysItr = store.storeKeysIterator(Direction.UNORDERED, 0, assembleKey(majorComponents, null), keyRange, null);
            }
        }

        return keysItr;
    }

    @Override
    public void destroy() {
        logger.info("Closing KVStore");
        if(store != null) {
            store.close();
            store = null;
        }
        logger.info("Closed KVStore");

    }

    public String getStoreName() {
        return storeName;
    }

    @Required
    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public String getHostName() {
        return hostName;
    }

    @Required
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getHostPort() {
        return hostPort;
    }

    @Required
    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public AvroCatalog getCatalog() {
        return catalog;
    }

    public void setCatalog(AvroCatalog catalog) {
        this.catalog = catalog;
    }

    public SpecificAvroBinding<SpecificRecord> getSpecificBinding() {
        return specificBinding;
    }

    public void setSpecificBinding(
            SpecificAvroBinding<SpecificRecord> specificBinding) {
        this.specificBinding = specificBinding;
    }

    public String[] getHostlist() {
        return hostlist;
    }

    public void setHostlist(String[] hostlist) {
        this.hostlist = hostlist;
    }
}
