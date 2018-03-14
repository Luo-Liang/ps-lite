#pragma once
#include <string.h>
#include <ps/Schedule.h>
#include <vector>
#include <ps/PLink.h>
class ScheduleBuilder
{
	PLinkKey key;
public:
	vector<shared_ptr<ScheduleNode>> CreateCollectives(vector<NodeId>& participants)
	{
		vector<shared_ptr<ScheduleNode>> results;
		for (Cntr rank = 0; rank < participants.size(); rank++)
		{
			var ops = make_shared<GlooHalvingAndDoubling<float>>();
			var ctx = make_shared<GlooContext>(participants, participants, rank, participants.size());
			results.push_back(make_shared<ScheduleNode>(ctx, ops, participants.at(rank)));
		}
		return results;
	}

	void IntelligentZip(vector<ScheduleNode> upstream, vector<ScheduleNode> downstream)
	{

	}
};