#include "../include/ps/PLink.h"
void PLinkExecutor::Initialize(
	unordered_map<PLinkKey, Schedule> schedules,
	string redezvousUri,
	unordered_map<NodeId, string> nodeToIP,
	vector<float>& sizes,
	vector<void*> applicationSuppliedAddrs,
	int totalParticipant,
	int elementWidth,
	NodeId Id)
{
	pHub = make_shared<PHub>(redezvousUri, 
		nodeToIP, 
		sizes, 
		applicationSuppliedAddrs, 
		totalParticipant, 
		elementWidth, 
		Id);
	pHub->InitializeDevice();
	//pHub->InitializeDeviceSpecifics();
	pHub->InitializePHubSpecifics();

	//rely on phub to probe machine configs
}