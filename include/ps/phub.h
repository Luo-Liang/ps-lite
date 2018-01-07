#pragma once
#include <vector>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <unordered_map>
using namespace std;
typedef uint32_t NodeId;
typedef int BufferHandle;
typedef int PHubKey;


class PHub
{
public:
	enum PHubRole
	{
		Worker,
		Server
	};
	vector<tuple<NodeId, PHubKey>> myPartitions;
	vector<tuple<PHubKey, NodeId>> destinations;
	unordered_map<NodeId, string> nodeMap;
	PHubRole role;
	//keys that i take care of

	PHub(vector<tuple<NodeId, PHubKey>> keys,
		vector<tuple<PHubKey, NodeId>> dests,
		unordered_map<NodeId,string> nodeToIP,
		PHubRole role);
	void Initialize();
	int Push(NodeId destination, BufferHandle buf);

};