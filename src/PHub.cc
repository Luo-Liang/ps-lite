#include "..\include\ps\phub.h"
#include "Verbs.hpp"
#include "zmq_van.h"

PHub::PHub(vector<tuple<NodeId, PHubKey>> keys,
	vector<tuple<PHubKey, NodeId>> dests,
	unordered_map<NodeId, string> nodeToIP,
	PHubRole myRole)
{
	myPartitions = keys;
	destinations = dests;
	nodeMap = nodeToIP;
	role = myRole;
	//now connect that.

}

void PHub::Initialize()
{
	//first, figure out devices.
	
}

int PHub::Push(NodeId destination, BufferHandle buf)
{
	return 0;
}
