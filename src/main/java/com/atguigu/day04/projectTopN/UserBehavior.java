package com.atguigu.day04.projectTopN;

/**
 * @author wangxin'ai
 * @Description // TODO POJO类
 * @createDate 2020-12-04 19:52
 */

//POJO类
public class UserBehavior {
    public String UserId;
    public String itemID;
    public String categoryId;
    public String behavior;
    public Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String itemID, String categoryId, String behavior, Long timestamp) {
        UserId = userId;
        this.itemID = itemID;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "UserId='" + UserId + '\'' +
                ", itemID='" + itemID + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
