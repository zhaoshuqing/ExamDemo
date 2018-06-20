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
	private static final HashMap<Integer, NodeInfo> hangUpTaskList = new HashMap<Integer, NodeInfo>();

	// 任务节点数据
	private static final HashMap<Integer, NodeInfo> taskInfoList = new HashMap<Integer, NodeInfo>();

	// 任务信息列表
	private static final List<TaskInfo> myTasks = new ArrayList<TaskInfo>();

	private int threshold;

	public int init() {
		nodeList.clear();
		hangUpTaskList.clear();
		taskInfoList.clear();
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
				hangUpTaskList.put(n.getTaskId(), n);
			}
		}

		nodeList.remove(nodeId);
		return ReturnCodeKeys.E006;

	}

	public int addTask(int taskId, int consumption) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
		if (hangUpTaskList.containsKey(taskId)) {
			return ReturnCodeKeys.E010;
		}
		NodeInfo addtaskNode = new NodeInfo(taskId, consumption);
		hangUpTaskList.put(taskId, addtaskNode);
		taskInfoList.put(taskId, addtaskNode);
		return ReturnCodeKeys.E008;

	}

	public int deleteTask(int taskId) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
		if (!taskInfoList.containsKey(taskId)) {
			return ReturnCodeKeys.E012;
		}
		// 删除挂起队列的任务
		if (hangUpTaskList.containsKey(taskId)) {
			hangUpTaskList.remove(taskId);
			return ReturnCodeKeys.E011;
		}

		NodeInfo taskNode = taskInfoList.get(taskId);
		List<NodeInfo> tempTaskList = nodeList.get(taskNode.getTaskId());
		if (tempTaskList != null && tempTaskList.size() > 0) {
			tempTaskList.remove(taskId);
		}
		taskInfoList.remove(taskId);
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
		if (hangUpTaskList.isEmpty() && nodeList.isEmpty())
			return ReturnCodeKeys.E013;
		this.threshold = threshold;
		boolean balanced = false;

		// 如果挂起任务中有任务
		if (!hangUpTaskList.isEmpty()) {

		}
		while (!balanced || hangUpTaskList.size() > 0) {
			for (Integer taskId : hangUpTaskList.keySet()) {
				int minNode = findMinServerNode();
				NodeInfo tempTask = hangUpTaskList.get(taskId);
				List<NodeInfo> minTaskList = nodeList.get(minNode);
				if (minTaskList == null) {
					minTaskList = new ArrayList<NodeInfo>();
				}
				minTaskList.add(new NodeInfo(tempTask.getTaskId(), tempTask.getConsumption()));
				hangUpTaskList.remove(taskId);
			}
		}

		return ReturnCodeKeys.E013;
	}

	public int queryTaskStatus(List<TaskInfo> tasks) {
		if (tasks == null || tasks.size() < 1) {
			return ReturnCodeKeys.E006;
		}

		for (Integer taskId : hangUpTaskList.keySet()) {
			TaskInfo t = new TaskInfo();
			t.setTaskId(taskId);
			t.setNodeId(-1);
			tasks.add(t);
		}
		for (TaskInfo temp : tasks) {
			TaskInfo t = new TaskInfo();
			t.setTaskId(temp.getTaskId());
			t.setNodeId(temp.getNodeId());
			tasks.add(temp);
		}

		Collections.sort(tasks, new Comparator<TaskInfo>() {
			public int compare(TaskInfo b1, TaskInfo b2) {
				if (b1.getTaskId() > (b2.getTaskId())) {
					return 1;
				} else if (b1.getTaskId() < (b2.getTaskId())) {
					return -1;
				}
				return 0;
			}

		});
		return ReturnCodeKeys.E015;
	}
}
