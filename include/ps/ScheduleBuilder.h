#pragma once
#include <string.h>
#include <ps/Schedule.h>
#include <vector>
#include <ps/PLink.h>
class ScheduleBuilder
{
	PLinkKey key;
public:
	ScheduleBuilder(PLinkKey key);
	vector<shared_ptr<ScheduleNode>> CreateCollectives(vector<NodeId>& participants);
	vector<shared_ptr<ScheduleNode>> IntelligentZip(vector<shared_ptr<ScheduleNode>>& upstream, vector<shared_ptr<ScheduleNode>>& downstream);
	vector<shared_ptr<ScheduleNode>> CreatePHubBroadcast(NodeId from, vector<NodeId>& To);
	vector<shared_ptr<ScheduleNode>> CreatePHubAggregatorInferredFromPHubBroadcastRecv(vector<shared_ptr<ScheduleNode>> nodes, vector<NodeId>& runOns);
	vector<shared_ptr<ScheduleNode>> CreatePHubOptimizerInferredFromAggOrCollectives(vector<shared_ptr<ScheduleNode>> nodes, vector<NodeId>& runOns);
	//figure out from a list of leaves, which ones are optimization. 
	vector<shared_ptr<ScheduleNode>> CreatePHubBroadcastInferredFromOptimizer(vector<shared_ptr<ScheduleNode>> nodes);
};