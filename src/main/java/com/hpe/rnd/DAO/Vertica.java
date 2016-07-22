package com.hpe.rnd.DAO;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by chzhenzh on 7/12/2016.
 */
public class Vertica {
    private Connection conn=null;
    private String url;
    private String user;
    private String password;
    private String schema;

    private Properties verticaProperties;
    private Statement stmt;
    public Vertica()
    {

    }
    public Vertica(String propertyFile) throws IOException, SQLException {
        verticaProperties = new Properties();
        verticaProperties.load(ClassLoader.getSystemResourceAsStream(propertyFile));
        url=verticaProperties.getProperty("url");
        user=verticaProperties.getProperty("user");
        password=verticaProperties.getProperty("password");
        schema=verticaProperties.getProperty("schema");
        conn = DriverManager.getConnection(url, user, password);
            // establish connection and make a table for the data.
        stmt = conn.createStatement();
            // Set AutoCommit to false to allow Vertica to reuse the same
        conn.setAutoCommit(true);
        Log.Info("Vertica connnected successfully,connection instance is:"+conn.toString());
    }

    public boolean checkExistTable(String tableName) throws SQLException {
        // meta.getTables(catalog, schemaPattern, tableNamePattern, type...
        DatabaseMetaData meta = null;
            meta = conn.getMetaData();
            ResultSet res = meta.getTables(null, schema, tableName, null);
            if(res.next()){
                return true;
            }else{
                return false;
            }
    }
    public void closeConn(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName,Object tableColumn[],String colunmLength,int columnNo) throws SQLException {
        //CREATE TABLE a(col1 varchar(100),col2 varchar(100);
        //java.sql.BatchUpdateException: [Vertica][VJDBC](4800) ERROR: String of 4443 octets is too long for type Varchar(1000)-->sometimes, message is too long
        String createTableString="CREATE TABLE "+schema+"."+tableName+"(";
        String createColumns=tableColumn[0]+" varchar("+colunmLength+")";
        for(int j=1;j<columnNo;j++)
        {
            String colStr=","+tableColumn[j]+" varchar("+colunmLength+")";
           // System.out.print(colStr);
            createColumns=createColumns+colStr;
        }
        createTableString=createTableString+createColumns+");";
        stmt.execute(createTableString);
        Log.Info(schema+"."+tableName+" is created.");
    }
    public void InsertInto(String tableName,Object tableColumn[],Object tableValues[],int columnNo) throws SQLException {

            //"INSERT INTO customers(last, first, id) VALUES(?,?,?)"
            String insertString = "INSERT INTO " + schema + "." + tableName + "(" + Arrays.toString(tableColumn).replace("[", "").replace("]", "") + ") VALUES(";
            String createColumns = "?";
            for (int j = 1; j < columnNo; j++) {
                String colStr = ",?";
                createColumns = createColumns + colStr;
            }
            insertString = insertString + createColumns + ")";
            PreparedStatement pstmt = conn.prepareStatement(insertString);
            for (int i = 0; i < columnNo; i++) {
                pstmt.setString(i + 1, tableValues[i].toString());
            }
            // Add row to the batch.
            pstmt.addBatch();
            pstmt.executeBatch();
}

    public void intoVertica(String tableName,Object tableColumn[],Object tableValues[],String colunmLength,int columnNo) throws SQLException {
        //System.out.println("Table "+schema+"."+tableName+" is there or not:" + checkExistTable(tableName));
        //resetColumnValue(tableColumn, tableValues);
        if(!checkExistTable(tableName)){
            createTable(tableName, tableColumn, colunmLength, columnNo);
        }
        InsertInto(tableName,tableColumn,tableValues, columnNo);
    }

    public void resetColumnValue(Object tableColumn[],Object tableValues[])
    {
        String msg[]=tableValues[0].toString().split(" ");
        String val[]=new String[msg.length];
        //<142>Jul 19 03:02:46 github haproxy[20100]: 16.49.191.11:56285 [19/Jul/2016:03:02:43.171] ssh_protocol babeld_ssh/localhost 1/0/3692 4429 -- 695/2/2/2/0 0/0
        String topicGroup=msg[3];
        String topicName=msg[4];
        //github babeld:
        if(topicGroup.indexOf(":")>=0){
            topicGroup="";
        }
        //column name start with V
        for(int i=0;i<msg.length;i++){
            String val0=msg[i];

            //check = is there or not
            if(val0.indexOf("=")>=0)
            {
                String splitval0[]=val0.split("=");
                val[i]=splitval0[1];
                msg[i] = splitval0[0];
            }else {
                val[i]=val0;
                msg[i] = "V" + i;

            }
        }
        //combine column, remove the fisrt message column

        // System.arraycopy();


    }
    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public Properties getVerticaProperties() {
        return verticaProperties;
    }

    public void setVerticaProperties(Properties verticaProperties) {
        this.verticaProperties = verticaProperties;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Statement getStmt() {
        return stmt;
    }

    public void setStmt(Statement stmt) {
        this.stmt = stmt;
    }
}
