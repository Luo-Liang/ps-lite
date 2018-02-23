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
	//spawn threads that are pinned to cores.
	var nthreads = pHub->machineConfig.CoreCount;
	var hc = std::thread::hardware_concurrency();
	var perSocket = hc / pHub->machineConfig.SocketCount;
	CHECK(nthreads <= hc);
	CHECK(nthreads % perSocket == 0);
	vector<int> socketTicketer(pHub->machineConfig.SocketCount);
	for (size_t i = 0; i < nthreads; i++)
	{
		int socketId = pHub->machineConfig.Core2SocketIdx.at(i);
		int socketOffset = socketTicketer.at(socketId);
		socketTicketer.at(socketId)++;
		int cpuid = socketId * perSocket + socketOffset;
		threads.push_back(thread(PLinkExecutor::Execute, this, i));
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(cpuid, &cpuset);
		int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
		if (rc != 0)
		{
			CHECK(false);
		}
		else
		{
			//printf("[PSHUB] mapping thread %d to cpuid %d, in socket = %d\n", i, cpuid, socketId);
		}
	}
	wQs = make_shared<PLinkWorkQueue>(sizes.size());
}

void PLinkExecutor::Execute(int tid)
{
	//this part is relatively easy.
	
}