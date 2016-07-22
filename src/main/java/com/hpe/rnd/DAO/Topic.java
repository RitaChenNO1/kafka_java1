package com.hpe.rnd.DAO;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by chzhenzh on 7/12/2016.
 */
public class Topic {
    private Properties topicProperties;
    private String topicsKeywords[];
    private String topics[];
    private String topicsString;
    private String topicsVerticaTables[];
    private String topicsStartKey;
    private String verticaTableColumnLength;
    private String messageOtherTopics;
    private String nonJsonTopics;
    private String nonMessageTopics;
    private int topicsNo;

    public Topic(){

    }
    public Topic(String propertyFile){
        topicProperties= new Properties();
        try {
            topicProperties.load(ClassLoader.getSystemResourceAsStream(propertyFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        topicsKeywords=topicProperties.getProperty("topicsKeywords").split(",");
        topics=topicProperties.getProperty("topics").split(",");
        topicsString=topicProperties.getProperty("topics");
        topicsVerticaTables=topicProperties.getProperty("topicsVerticaTables").split(",");
        topicsStartKey=topicProperties.getProperty("topicsStartKey");
        messageOtherTopics=topicProperties.getProperty("messageOtherTopics");
        nonJsonTopics=topicProperties.getProperty("nonJsonTopics");
        nonMessageTopics=topicProperties.getProperty("nonMessageTopics");
        topicsNo=topicsKeywords.length<=topics.length?topicsKeywords.length:topics.length;
        verticaTableColumnLength=topicProperties.getProperty("verticaTableColumnLength");
    }

    public Properties getTopicProperties() {
        return topicProperties;
    }

    public void setTopicProperties(Properties topicProperties) {
        this.topicProperties = topicProperties;
    }

    public String[] getTopicsKeywords() {
        return topicsKeywords;
    }

    public void setTopicsKeywords(String[] topicsKeywords) {
        this.topicsKeywords = topicsKeywords;
    }

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    public String getTopicsStartKey() {
        return topicsStartKey;
    }

    public void setTopicsStartKey(String topicsStartKey) {
        this.topicsStartKey = topicsStartKey;
    }

    public String getMessageOtherTopics() {
        return messageOtherTopics;
    }

    public void setMessageOtherTopics(String messageOtherTopics) {
        this.messageOtherTopics = messageOtherTopics;
    }

    public String getNonJsonTopics() {
        return nonJsonTopics;
    }

    public void setNonJsonTopics(String nonJsonTopics) {
        this.nonJsonTopics = nonJsonTopics;
    }

    public String getNonMessageTopics() {
        return nonMessageTopics;
    }

    public void setNonMessageTopics(String nonMessageTopics) {
        this.nonMessageTopics = nonMessageTopics;
    }

    public String getTopicsString() {
        return topicsString;
    }

    public void setTopicsString(String topicsString) {
        this.topicsString = topicsString;
    }

    public int getTopicsNo() {
        return topicsNo;
    }

    public void setTopicsNo(int topicsNo) {
        this.topicsNo = topicsNo;
    }

    public String[] getTopicsVerticaTables() {
        return topicsVerticaTables;
    }

    public void setTopicsVerticaTables(String[] topicsVerticaTables) {
        this.topicsVerticaTables = topicsVerticaTables;
    }

    public String getVerticaTableColumnLength() {
        return verticaTableColumnLength;
    }

    public void setVerticaTableColumnLength(String verticaTableColumnLength) {
        this.verticaTableColumnLength = verticaTableColumnLength;
    }
}
