#pragma once
#include <string.h>
#include <ps/Schedule.h>
#include <vector>
class ScheduleBuilder
{
	PLinkKey Key;
	shared_ptr<ScheduleNode> pChainHead = NULL;
	shared_ptr<ScheduleNode> pChainTrail = NULL;
public:
	ScheduleBuilder(PLinkKey key)
	{
		Key = key;

	}
	//chains
	ScheduleBuilder ThenRunCollective(vector<NodeId> inputs, std::string algorithm);
	ScheduleBuilder ThenRunPHubBroadcast(NodeId from, vector<NodeId> to);
	ScheduleBuilder ThenRunPHubGather(vector<NodeId> from, NodeId to);
	ScheduleBuilder ThenRunPHubAggregate(vector<NodeId> runOn);
	ScheduleBuilder ThenRunPHubOptimize(vector<NodeId> runOn);
	//chain heads
	ScheduleBuilder HeadRunCollective(vector<NodeId> inputs, std::string algorithm);
	ScheduleBuilder HeadRunPHubBroadcast(NodeId from, vector<NodeId> to);
	ScheduleBuilder HeadRunPHubGather(vector<NodeId> from, NodeId to);
	//zip two chains.
	ScheduleBuilder ZipAll(vector<NodeId> inputs);
};