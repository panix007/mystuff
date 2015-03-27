package com.panicker.nosql.dao;

import com.panicker.resolve.pmf.avro.PMFPromotion;
import com.panicker.dto.pmf.Promotions;
import oracle.kv.*;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.SpecificAvroBinding;
import org.apache.avro.specific.SpecificRecord;

import java.util.Arrays;
import java.util.List;

public class NoSQLDao {

    String storeName = "kvstore";
    String hostName = "localhost";
    String hostPort = "5000";
    String schema = "promotions.avsc";

    KVStore kvStore = null;

    public NoSQLDao(){
        log(storeName + " - " + hostName + ":" + hostPort);
        KVStoreConfig config = new KVStoreConfig(storeName, hostName + ":" + hostPort);
        kvStore = KVStoreFactory.getStore(config);
    }

    public boolean persist(List<String> majorPath, List<String> minorPath, SpecificRecord specificRecord){
        Key key = createKey(majorPath, minorPath);
        Value value = getSpecificBindings().toValue(specificRecord);
        kvStore.put(key, value);
        log("persist: Key: "+majorPath+":"+minorPath+" Value: "+value);
        return true;
    }

    public void persist(List<String> majorPath, List<String> minorPath, String data){
        Key key = createKey(majorPath, minorPath);
        Value value = getValue(data);
        kvStore.put(key, value);
        log("persist: Key: "+majorPath+":"+minorPath+" Value: "+data);
    }

    public void persistIfAbsent(List<String> majorPath, List<String> minorPath, String data){
        Key key = createKey(majorPath, minorPath);
        Value value = getValue(data);
        kvStore.putIfAbsent(key, value);
        log("persistIfAbsent: Key: "+majorPath+":"+minorPath+" Value: "+data);
    }

    public void persistIfPresent(List<String> majorPath, List<String> minorPath, String data){
        Key key = createKey(majorPath, minorPath);
        Value value = getValue(data);
        kvStore.putIfPresent(key, value);
        log("persistIfPresent: Key: "+majorPath+":"+minorPath+" Value: "+data);
    }

    public String fetchValue(List<String> majorPath, List<String> minorPath){
        Key key = createKey(majorPath, minorPath);
        ValueVersion valueVersion = kvStore.get(key);
        Value value = valueVersion.getValue();
        byte[] bytes = value.getValue();
        String valueText = new String(bytes);
        log("fetchValue: Key: "+majorPath+":"+minorPath+" Value: "+valueText);
        return valueText;
    }


    public void crudOperation(){
        // Login modeled as /Login/$userName: $password
        final String userName = "ppanicker";
        final String password = "welcome";

        // Create a login for Jasper
        List<String> majorPath = Arrays.asList("Login", userName);

        persistIfAbsent(majorPath, null, password);
        fetchValue(majorPath, null);

        List<String> minorPath = Arrays.asList("age");
        persistIfAbsent(majorPath, minorPath, "36");
        fetchValue(majorPath, minorPath);

        PMFPromotion pmfPromotion = new PMFPromotion();
        pmfPromotion.setChannelClass("Channel Class");
        pmfPromotion.setCrmProductType("CRM Product Type");
        pmfPromotion.setFulfillmentType("Fulfillment Type");
        pmfPromotion.setItemDesc("Item Desc");
        pmfPromotion.setLongDesc("Long Desc");
        pmfPromotion.setRevision("Revision");
        pmfPromotion.setTariffCode("Tariff Code");

        majorPath = Arrays.asList("promotion", "sc123456");
        minorPath = Arrays.asList("pr123456");
        persist(majorPath, minorPath, pmfPromotion);

        fetchValue(majorPath, minorPath);
    }

    private SpecificAvroBinding<SpecificRecord> getSpecificBindings(){
        //Create SpecificBindings for Member schemas
        AvroCatalog catalog = kvStore.getAvroCatalog();
        SpecificAvroBinding<SpecificRecord> specificBinding = catalog.getSpecificMultiBinding();
        return specificBinding;
    }

    private Key createKey(List<String> majorPath, List<String> minorPath){
        Key key = null;
        if (minorPath == null || minorPath.isEmpty()){
            key = Key.createKey(majorPath);
        } else {
            key = Key.createKey(majorPath, minorPath);
        }

        return key;
    }

    private Value getValue(String data){
        byte[] bytes = data.getBytes();
        Value value = Value.createValue(bytes);
        return value;
    }

    public static void main (String[] args){
        long initiationTime = System.currentTimeMillis();
        NoSQLDao noSQLDao = new NoSQLDao();
        long startTime = System.currentTimeMillis();
        noSQLDao.crudOperation();
        long endTime = System.currentTimeMillis();
        noSQLDao.log("Time taken: "+(endTime - startTime)+" msecs");
        noSQLDao.log("Total Time taken: "+(endTime - initiationTime)+" msecs");
    }

    public void log(String logData){
        System.out.println(logData);
    }
}
