package com.migu.schedule.info;

public class NodeInfo
{
    private int taskId;
    private int consumption;
    
    /**
     * 构造函数 
     * @param taskId
     * @param consumption
     */
    public NodeInfo(int taskId, int consumption)
    {
        super();
        this.taskId = taskId;
        this.consumption = consumption;
    }
    /**
     * 取得taskId
     * @return 返回taskId。
     */
    public int getTaskId()
    {
        return taskId;
    }
    /**
     * 设置taskId
     * @param taskId 要设置的taskId。
     */
    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
    }
    /**
     * 取得consumption
     * @return 返回consumption。
     */
    public int getConsumption()
    {
        return consumption;
    }
    /**
     * 设置consumption
     * @param consumption 要设置的consumption。
     */
    public void setConsumption(int consumption)
    {
        this.consumption = consumption;
    }
    
    
}
