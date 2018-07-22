package org.apache.kafka.connect.runtime.distributed;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.runtime.ConfigCmdType;
import org.apache.kafka.connect.util.ConnectorTaskId;

public class confighelper {
	
    public static boolean judgeConnectorStartAndSend(ConnectProtocol.Assignment runtimeinfo,String ConnectorName,List<configcmd> q) {
    	boolean bRet = true;
    	if(!runtimeinfo.connectors().contains(ConnectorName)) {
    		configcmd cmd = new configcmd();
    		cmd.cmdtype = ConfigCmdType.CONNECTOR_START;
    		cmd.param = ConnectorName;
    		q.add(cmd);
    	}else {
    		bRet = false;
    	}
    	return bRet;
    }
    
    public static boolean judgeConnectorStopAndSend(ConnectProtocol.Assignment runtimeinfo,String ConnectorName,List<configcmd> q) {
    	boolean bRet = true;
    	if(runtimeinfo.connectors().contains(ConnectorName)) {
    		configcmd cmd = new configcmd();
    		cmd.cmdtype = ConfigCmdType.CONNECTOR_STOP;
    		cmd.param = ConnectorName;
    		q.add(cmd);
    	}else {
    		bRet = false;
    	}
    	return bRet;
    }
    
    public static boolean JudgeTaskStartAndSend(ConnectProtocol.Assignment runtimeinfo,ConnectorTaskId Task,List<configcmd> q) {
    	boolean bRet = true;
    	if(!runtimeinfo.tasks().contains(Task)) {
    		configcmd cmd = new configcmd();
    		cmd.cmdtype = ConfigCmdType.TASK_START;
    		cmd.param = Task;
    		q.add(cmd);
    	}else {
    		bRet = false;
    	}
        return bRet;
    }
    
    public static boolean JudgeTaskStopAndSend(ConnectProtocol.Assignment runtimeinfo,ConnectorTaskId Task,List<configcmd> q) {
    	boolean bRet = true;
    	if(runtimeinfo.tasks().contains(Task)) {
    		configcmd cmd = new configcmd();
    		cmd.cmdtype = ConfigCmdType.TASK_STOP;
    		cmd.param = Task;
    		q.add(cmd);
    	}else {
    		bRet = false;
    	}
        return bRet;
    }
    
    public static void AddConnectorToRuntimeInfo(String ConnectorName,ConnectProtocol.Assignment runtimeinfo,WorkerGroupMember m) {
    	runtimeinfo.AddConnector(ConnectorName);
    	m.setassignmentSnapshot(runtimeinfo);
    }

    public static void DelConnectorFromRuntimeInfo(String ConnectorName,ConnectProtocol.Assignment runtimeinfo,WorkerGroupMember m) {
    	runtimeinfo.RemoveConnector(ConnectorName);
    	m.setassignmentSnapshot(runtimeinfo);
    }
    
    public static void AddTaskToRuntimeInfo(ConnectorTaskId Task,ConnectProtocol.Assignment runtimeinfo,WorkerGroupMember m) {
    	runtimeinfo.AddTask(Task);
    	m.setassignmentSnapshot(runtimeinfo);
    }
    
    public static void DelTaskFromRuntimeInfo(ConnectorTaskId Task,ConnectProtocol.Assignment runtimeinfo,WorkerGroupMember m) {
    	runtimeinfo.RemoveTask(Task);
    	m.setassignmentSnapshot(runtimeinfo);
    }
    
}
