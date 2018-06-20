package com.migu.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.NodeInfo;
import com.migu.schedule.info.TaskInfo;

/*
*类名和方法不能修改
 */
public class Schedule {

	// 服务节点列表
	private static final HashMap<Integer, List<NodeInfo>> nodeList = new HashMap<Integer, List<NodeInfo>>();

	// 挂起队列
	private static final HashMap<Integer, NodeInfo> hangUpTaskMap = new HashMap<Integer, NodeInfo>();

	// 任务节点数据
	private static final HashMap<Integer, NodeInfo> taskInfoMap = new HashMap<Integer, NodeInfo>();

	// 任务信息列表
	private static final List<TaskInfo> myTasks = new ArrayList<TaskInfo>();

	private int threshold;

	public int init() {
		nodeList.clear();
		hangUpTaskMap.clear();
		taskInfoMap.clear();
		myTasks.clear();
		return ReturnCodeKeys.E001;
	}

	public int registerNode(int nodeId) {
		if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}

		if (nodeList.containsKey(nodeId)) {
			return ReturnCodeKeys.E005;
		}

		nodeList.put(nodeId, null);
		return ReturnCodeKeys.E003;
	}

	public int unregisterNode(int nodeId) {
		if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}

		if (!nodeList.containsKey(nodeId)) {
			return ReturnCodeKeys.E007;
		}

		List<NodeInfo> supTasks = nodeList.get(nodeId);
		if (supTasks != null && supTasks.size() > 0) {
			for (NodeInfo n : supTasks) {
				hangUpTaskMap.put(n.getTaskId(), n);
			}
		}

		nodeList.remove(nodeId);
		return ReturnCodeKeys.E006;

	}

	public int addTask(int taskId, int consumption) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
		if (hangUpTaskMap.containsKey(taskId)) {
			return ReturnCodeKeys.E010;
		}
		NodeInfo addtaskNode = new NodeInfo(taskId, consumption);
		hangUpTaskMap.put(taskId, addtaskNode);
		taskInfoMap.put(taskId, addtaskNode);
		return ReturnCodeKeys.E008;

	}

	public int deleteTask(int taskId) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
		if (!taskInfoMap.containsKey(taskId)) {
			return ReturnCodeKeys.E012;
		}
		// 删除挂起队列的任务
		if (hangUpTaskMap.containsKey(taskId)) {
			hangUpTaskMap.remove(taskId);
			return ReturnCodeKeys.E011;
		}

		NodeInfo taskNode = taskInfoMap.get(taskId);
		List<NodeInfo> tempTaskList = nodeList.get(taskNode.getTaskId());
		if (tempTaskList != null && tempTaskList.size() > 0) {
			tempTaskList.remove(taskId);
		}
		taskInfoMap.remove(taskId);
		return ReturnCodeKeys.E011;

	}

	/**
	 * 
	 * 统计服务节点里的消耗率
	 */
	private int sumConsumption(List<NodeInfo> nodeInfos) {
		int sum = 0;
		for (NodeInfo nodeInfo : nodeInfos) {
			sum += nodeInfo.getConsumption();
		}
		return sum;
	}

	/**
	 * 
	 * 找出总消耗量最小的服务器节点
	 *
	 * @return
	 */
	private int findMinServerNode() {
		int result = -1;
		int start = 99;
		for (Integer nodeId : nodeList.keySet()) {
			result = nodeId;
			List<NodeInfo> taskNodes = nodeList.get(nodeId);
			if (taskNodes != null && taskNodes.size() > 0) {
				int sumConsum = sumConsumption(taskNodes);
				start = sumConsum;
				if (sumConsum < start) {
					start = sumConsum;
					result = nodeId;
				}
			}
		}
		return result;
	}

	public int scheduleTask(int threshold) {
		if (threshold <= 0)
			return ReturnCodeKeys.E002;
		// 没有服务节点和挂起任务，返回成功
		if (hangUpTaskMap.isEmpty() && nodeList.isEmpty())
			return ReturnCodeKeys.E013;
		this.threshold = threshold;
		boolean balanced = false;

		// 如果挂起任务中有任务
		if (!hangUpTaskMap.isEmpty()) {

		}
		while (!balanced || hangUpTaskMap.size() > 0) {
			for (Integer taskId : hangUpTaskMap.keySet()) {
				int minNode = findMinServerNode();
				NodeInfo tempTask = hangUpTaskMap.get(taskId);
				List<NodeInfo> minTaskList = nodeList.get(minNode);
				if (minTaskList == null) {
					minTaskList = new ArrayList<NodeInfo>();
				}
				minTaskList.add(new NodeInfo(tempTask.getTaskId(), tempTask.getConsumption()));
				hangUpTaskMap.remove(taskId);
			}
		}

		return ReturnCodeKeys.E013;
	}

	public int queryTaskStatus(List<TaskInfo> tasks) {
		if (tasks == null || tasks.size() < 1)
		{
			return ReturnCodeKeys.E006;
		}
			
		List<TaskInfo> tempList = new ArrayList<TaskInfo>();
		for (TaskInfo task : tasks) {
			
			if(taskInfoMap.containsKey(task.getTaskId()))
			{
				if(hangUpTaskMap.containsKey(task.getTaskId()))
				{
					task.setNodeId(-1);
					
				}
				tempList.add(task);
				continue;
			}
		}
		
		tasks.clear();
		tasks.addAll(tempList);
		
		Collections.sort(tasks,new Comparator<TaskInfo>(){
            public int compare(TaskInfo b1, TaskInfo b2) {
                 if(b1.getTaskId()>(b2.getTaskId()))
                 {
                     return 1;
                 }
                 else if(b1.getTaskId()<(b2.getTaskId()))
                {
                    return -1;
                }
                return 0;
            }

        });
		return ReturnCodeKeys.E015;
	}
}
