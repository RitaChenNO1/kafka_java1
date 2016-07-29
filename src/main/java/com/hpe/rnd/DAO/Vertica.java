package com.hpe.rnd.DAO;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by chzhenzh on 7/12/2016.
 */
public class Vertica {
    private Connection conn = null;
    private String url;
    private String user;
    private String password;
    private String schema;

    private Properties verticaProperties;
    private Statement stmt;

    public Vertica() {

    }

    public Vertica(String propertyFile) throws IOException, SQLException {
        verticaProperties = new Properties();
        verticaProperties.load(ClassLoader.getSystemResourceAsStream(propertyFile));
        url = verticaProperties.getProperty("url");
        user = verticaProperties.getProperty("user");
        password = verticaProperties.getProperty("password");
        schema = verticaProperties.getProperty("schema");
        conn = DriverManager.getConnection(url, user, password);
        // establish connection and make a table for the data.
        stmt = conn.createStatement();
        // Set AutoCommit to false to allow Vertica to reuse the same
        conn.setAutoCommit(true);
        Log.Info("Vertica connnected successfully,connection instance is:" + conn.toString());
    }

    public boolean checkExistTable(String tableName) throws SQLException {
        // meta.getTables(catalog, schemaPattern, tableNamePattern, type...
        DatabaseMetaData meta = null;
        meta = conn.getMetaData();
        ResultSet res = meta.getTables(null, schema, tableName, null);
        if (res.next()) {
            return true;
        } else {
            return false;
        }
    }

    //    zoey add on 7/22
    public Object[] splitMessage(Object jsonKeys[], Object jsonValues[], String tableName) throws SQLException {

        Map m1 = new HashMap();
        Object kv[] = new Object[2];
        Object msgValue = jsonValues[0];

        Boolean match = Pattern.compile("[^a-zA-Z0-9_]").matcher(msgValue.toString()).replaceAll("").trim().indexOf(tableName) >= 0;
//        System.out.println("contains tablename or not : " + match);


        if (match) {
            String msg[] = msgValue.toString().split(" ");

            int i = 0;
            //to get m1={"message_v0":"","topicGroup":"","topicName":"","message_v1":""}

            for (i = 0; i < msg.length; i++) {

                String regmsg = Pattern.compile("[^a-zA-Z0-9_]").matcher(msg[i]).replaceAll("").trim();
                if (regmsg.contains(tableName)) {

                    m1.put("topicName", msg[i]);

                    if (msg[i - 1].replace(":", "").matches("[0-9]{1,}")) {
                        m1.put("topicGroup", "");

                    } else {
                        m1.put("topicGroup", msg[i - 1]);
                    }
//                    System.out.println("currently, m1 should have 2 parts, topicGroup,topicName");
//                    System.out.println(m1.keySet());
//                    System.out.println(m1.values());
//                    System.out.println(msg[i - 1].concat(" ").concat(msg[i]));

                    String keyGrouop[] = msgValue.toString().split(Pattern.quote(msg[i - 1].concat(" ").concat(msg[i])));
                    System.out.println("the length must be 2 : " + keyGrouop.length);
                    System.out.println(Arrays.toString(keyGrouop));
                    m1.put("message_v0", keyGrouop[0]);
                    m1.put("message_v1", keyGrouop[1]);
//                    System.out.println(m1);
                    break;
                }

            }
            if (m1.size() != 4) {
                System.out.println("Generage topicName, topicGroup, message_v0,message_v1 not succeed.");
            }
//            System.out.println("*************first m1, contains 4 parts***************");
//            System.out.println(m1.keySet());
//            System.out.println(m1.values());
            //end for , we get m1, then go to split "message_v1",if message_v1 contains "="
            String message_v1 = m1.get("message_v1").toString();
//            System.out.println("***********this is message_v1*************");
//            System.out.println(message_v1);
            if (message_v1.indexOf("=") >= 0) {
                String lastKey = new String("otherMessage");
                //if the left of "=" contains special characters
                String equal_string[] = message_v1.split(" ");
                int j = 0;
                for (j = 0; j < equal_string.length; j++) {

                    if (equal_string[j].indexOf("=") >= 0) {
                        String str[] = equal_string[j].split("=");
                        //the left character of = doesn't contain special character
                        if (!isConSpeCharacters(str[0])) {
                            String left = StringFilter(str[0]);
                            String right = "null";
                            if (str.length != 1) {
                                right = str[1];

                            }

                            lastKey = "message_".concat(left);
                            m1.put(lastKey, right);
                        } else {
                            //the equal_string[i] left contain special character, then no need to add it to a column

                            if (m1.containsKey(lastKey)) {
                                m1.put(lastKey, m1.get(lastKey).toString().concat(" ").concat(equal_string[j]));

                            } else {
                                m1.put(lastKey, equal_string[j]);
                            }

                        }


                    } else {
                        //the equal_string[i] doesn't contain "=", then concate it to a string

                        if (m1.containsKey(lastKey)) {
                            m1.put(lastKey, m1.get(lastKey).toString().concat(" ").concat(equal_string[j]));

                        } else {
                            m1.put(lastKey, equal_string[j]);
                        }

                    }


                }

                //update m1
                m1.remove("message_v1");
                kv[0] = insertArray(jsonKeys, m1.keySet().toArray());
                kv[1] = insertArray(jsonValues, m1.values().toArray());


            } else {
                m1.put("otherMessage", m1.get("message_v1"));
                m1.remove("message_v1");

                kv[0] = insertArray(jsonKeys, m1.keySet().toArray());

                kv[1] = insertArray(jsonValues, m1.values().toArray());

            }


        } else {
            System.out.println("message value doesn't contain TableName, go to find root cause");
        }
//<142>Jul 19 03:02:46 github haproxy[20100]: 16.49.191.11:56285 [19/Jul/2016:03:02:43.171] ssh_protocol babeld_ssh/localhost 1/0/3692 4429 -- 695/2/2/2/0 0/0

//update jsonKeys and jsonValues then insert into Vertica


        return kv;
    }

    public String StringFilter(String str) throws PatternSyntaxException {
        // 只允许字母和数字
        // String   regEx  =  "[^a-zA-Z0-9]";
        // 清除掉所有特殊字符
        String regEx = "[`~!@#$%^&*()+=|{}':;',\"\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }

    private boolean isConSpeCharacters(String string) {
        // TODO Auto-generated method stub
        String regEx = "[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";
        if (string.replaceAll(regEx, "").length() != string.length()) {
            //如果不包含特殊字符
            return true;
        }
        return false;
    }


    //get the existing column of specific table
    public String[] getCols(String tableName) throws SQLException {
        DatabaseMetaData meta = null;
        meta = conn.getMetaData();
        ResultSet res = stmt.executeQuery("SELECT * FROM " + schema + "." + tableName + " limit 1;");
        ResultSetMetaData rs = res.getMetaData();
        int cols = rs.getColumnCount();
        String tableColumn[] = new String[cols];
        for (int i = 1; i <= cols; i++) {
            //获取指定咧的名称
            tableColumn[i - 1] = rs.getColumnName(i);
        }
        return tableColumn;
    }

    //    zoey add on 7/22
    //check whether msgCol is subset of tableColumn or not
    public boolean checkColumns(String[] tableColumn, String[] msgCol) throws SQLException {
        List listA = Arrays.asList(tableColumn);
        List listB = Arrays.asList(msgCol);
        return listA.containsAll(listB);
    }

    //    zoey add on 7/22
    //if the msgCol is not the subset of tableColumn , should add new column to the table
    public void alterTable(String tableName, String colName,String colunmLength) throws SQLException {

        Statement stmt = conn.createStatement();

        String query1 = "ALTER TABLE " + schema + "." + tableName + " ADD " + colName + " VARCHAR("+colunmLength+");";

        stmt.execute(query1);
        Log.Info(schema + "." + tableName + " add column " + colName);
    }

    //    zoey add on 7/22
    //add functions to compute the intersect and difference subset of String Arrray A and String Array B
    //    String Array operation
    //求两个数组的交集
    public String[] intersect(String[] arr1, String[] arr2) {
        Map<String, Boolean> map = new HashMap<String, Boolean>();
        LinkedList<String> list = new LinkedList<String>();
        for (String str : arr1) {
            if (!map.containsKey(str)) {
                map.put(str, Boolean.FALSE);
            }
        }
        for (String str : arr2) {
            if (map.containsKey(str)) {
                map.put(str, Boolean.TRUE);
            }
        }

        for (Map.Entry<String, Boolean> e : map.entrySet()) {
            if (e.getValue().equals(Boolean.TRUE)) {
                list.add(e.getKey());
            }
        }

        String[] result = {};
        return list.toArray(result);
    }

    //    zoey add on 7/22
    //求两个数组的差集
    public String[] minus(String[] arr1, String[] arr2) {
        LinkedList<String> list = new LinkedList<String>();
        LinkedList<String> history = new LinkedList<String>();
        String[] longerArr = arr1;
        String[] shorterArr = arr2;
        //找出较长的数组来减较短的数组
        if (arr1.length > arr2.length) {
            longerArr = arr2;
            shorterArr = arr1;
        }
        for (String str : longerArr) {
            if (!list.contains(str)) {
                list.add(str);
            }
        }
        for (String str : shorterArr) {
            if (list.contains(str)) {
                history.add(str);
                list.remove(str);
            } else {
                if (!history.contains(str)) {
                    list.add(str);
                }
            }
        }

        String[] result = {};
        return list.toArray(result);
    }

    //    zoey add on 7/22
    private String[] insertElement(String original[],
                                   String element, int index) {
        int length = original.length;
        String destination[] = new String[length + 1];
        System.arraycopy(original, 0, destination, 0, index);
        destination[index] = element;
        System.arraycopy(original, index, destination, index
                + 1, length - index);
        return destination;
    }

    //    zoey add on 7/22
    private Object[] insertArray(Object original[], Object element[]) {
        Object destination[] = new Object[original.length + element.length - 1];
        System.arraycopy(element, 0, destination, 0, element.length);
        System.arraycopy(original, 1, destination, element.length, original.length - 1);
        return destination;
    }


    public void closeConn() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, Object tableColumn[], String colunmLength, int columnNo) throws SQLException {
        //CREATE TABLE a(col1 varchar(100),col2 varchar(100);
        //java.sql.BatchUpdateException: [Vertica][VJDBC](4800) ERROR: String of 4443 octets is too long for type Varchar(1000)-->sometimes, message is too long
        String createTableString = "CREATE TABLE " + schema + "." + tableName + "(";
        String createColumns = tableColumn[0] + " varchar(" + colunmLength + ")";
        for (int j = 1; j < columnNo; j++) {
            String colStr = "," + tableColumn[j] + " varchar(" + colunmLength + ")";
            // System.out.print(colStr);
            createColumns = createColumns + colStr;
        }
        createTableString = createTableString + createColumns + ");";
        stmt.execute(createTableString);
        Log.Info(schema + "." + tableName + " is created.");
    }

    public void InsertInto(String tableName, Object tableColumn[], Object tableValues[], int columnNo) throws SQLException {

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

    //    Zoey Modify on 7/22
    public void intoVertica(String tableName, Object tableColumn[], Object tableValues[], String colunmLength, int columnNo) throws SQLException {
        //System.out.println("Table "+schema+"."+tableName+" is there or not:" + checkExistTable(tableName));
        //resetColumnValue(tableColumn, tableValues);
        if (!checkExistTable(tableName)) {
            createTable(tableName, tableColumn, colunmLength, columnNo);
        } else {
            String tableCols[] = getCols(tableName);
            String msgCol[] = new String[tableColumn.length];
            for (int i = 0; i < tableColumn.length; i++) {
                msgCol[i] = tableColumn[i].toString();
            }
//            System.out.println("********************************************");
//            System.out.println("tableCols=" + Arrays.toString(tableCols));
//            System.out.println("msgCol=" + Arrays.toString(msgCol));
//            System.out.println("checkColumns=" + checkColumns(tableCols, msgCol));
//            System.out.println("intersect=" + Arrays.toString(intersect(tableCols, msgCol)));
//            System.out.println("minus=" + Arrays.toString(minus(msgCol, intersect(tableCols, msgCol))));
//            System.out.println("********************************************");

            if (!checkColumns(tableCols, msgCol)) {
                //msgCol isn't the subset of tableColumn, then compute the difference set of msgCol and tableColumn
                String insubset[] = intersect(tableCols, msgCol);
                String diffsubset[] = minus(msgCol, insubset);
                //alter table by adding columns one by one
                for (String str : diffsubset) {
                    alterTable(tableName, str,colunmLength);
                }
            } else {
                Log.Info(schema + "." + tableName + " exist and columns are match!");
            }
        }
        InsertInto(tableName, tableColumn, tableValues, columnNo);
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
