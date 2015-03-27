package com.panicker.nosql;

import com.panicker.kvsupport.KVAccessor;
import com.panicker.nosql.dao.NoSQLDao;

/**
 * Created by PP0063638 on 3/27/2015.
 */
public class Persister {

    NoSQLDao noSQLDao = null;

    public Persister(){
        noSQLDao = new NoSQLDao();
    }

    public void persistToDB(String schema, String data){

    }

    public void persistToDB(String schema, String[] data){

    }


}
