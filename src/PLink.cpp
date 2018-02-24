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
		CHECK(rc == 0);
	}
	wQs = make_shared<PLinkWorkQueue>(sizes.size());
}

void PLinkExecutor::Execute(int tid)
{
	//this part is relatively easy.
	var key2Devs = pHub->RetriveKey2DevMap();
	//figure out which key am i in charge with?
	vector<int> myKeys; //make a copy
	for (Cntr i = 0; i < key2Devs.size(); i++)
	{
		var core = key2Devs.at(i);
		if (tid == core)
		{
			myKeys.push_back(i);
		}
	}

	//scan ready tasks then execute.
	while (gtg == false)
	{
		for (var idx : myKeys)
		{
			if (wQs->WorkQueues[idx] != NULL)
			{
				var result = wQs->WorkQueues[idx]->pOperator->Run();
				if (result == OperationStatus::Finished)
				{
					//flush this,queue my downstream.
					//it seems true that per key dependency is linear
					//is there anything else to do?
					if (wQs->WorkQueues[idx]->Downstream.size() != 0)
					{
						//only 1 dependency.
						CHECK(wQs->WorkQueues[idx]->Downstream.size() == 1);
						wQs->WorkQueues[idx] = wQs->WorkQueues[idx]->Downstream.front();
					}
					else
					{
						//nothing to do. set to null.
						//but who is going to readify the graph?
						wQs->WorkQueues[idx] = NULL;
					}
					//
				}
				//tasks that have not finished must not have side-effects:poll again
			}
		}
	}
}