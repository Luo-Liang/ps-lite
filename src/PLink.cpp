#include "../include/ps/PLink.h"
void PLinkExecutor::ReadiyGraph()
{
	//gloo::transport::Device 
	//I only care about my schedule

	for (var pair : perKeySchedule)
	{
		var key = pair.first;
		var& schedule = pair.second;
		currentNodePerKeySchedule.at(key) = schedule->Filter(ID);
		wQs->WorkQueues.at(key) = currentNodePerKeySchedule.at(key).at(0);

		for (var& step : currentNodePerKeySchedule.at(key))
		{
			var pctx = step->pContext;
			var op = step->pOperator;

		}
	}

	//now take care of Gloo
	for (auto& step : mySchedule->Components)
	{
		var pctx = step->pContext;
		var op = step->pOperator;
		std::sort(pctx->inputs.begin(), pctx->inputs.end());
		std::sort(pctx->outputs.begin(), pctx->outputs.end());
		switch (op->Type)
		{
			case GlooCollectiveAlgorithm:
			{
				//here we need to initialize lots of gloo contexts.
				//how many people i need to synchronize with?
				//first, check inputs and outputs are the same.
				//collectives only synchronize to the same nodes.

				//am I in this step?
				//gloo expects different ranks than us.
				CHECK(pctx->inputs.size() == pctx->outputs.size());
				CHECK(pctx->inputs == pctx->outputs);
				//check input lens are identical, for gloo.
				CHECK(pctx->typeCode == OperatorContext::OperatorContextTypeCode::LocallyAvailable);
				//figure out who are the nodes.
				vector<NodeId> nodes;
				for (auto handle : pctx->inputs)
				{
					//inputs to gloo must be all local
					CHECK(NodeIdFromHandle(handle) == ID);
					nodes.push_back(NodeIdFromHandle(handle));
				}
				std::sort(nodes.begin(), nodes.end());
				auto idx = CxxxxBinarySearch(nodes.begin(), nodes.end(), ID);
				//figure out what keys are needed locally?
				//these keys are in inputs.
				std::shared_ptr<gloo::rendezvous::Context> pContext = std::make_shared<gloo::rendezvous::Context>(idx, pctx->inputs.size());
				pctx->additionalContext = pContext;
				//attempt to connect to this mesh
				pContext->connectFullMesh(*pRedisStore, pGlooDefaultDevice);
				//create this context.
			}
			default:
			{
				CHECK(false) << " Not implemented.";
			}
		}


		//make sure everyone has executed this step.
		//barrier
		//phubRendezvous->SynchronousBarrier(op->GetUniqueName(), totalPHubNodes);

	}
}

void PLinkExecutor::Initialize(
	unordered_map<PLinkKey, shared_ptr<Schedule>> schedules,
	string redezvousUri,
	unordered_map<NodeId, string> nodeToIP,
	vector<float>& sizes,
	vector<void*> applicationSuppliedAddrs,
	int totalParticipant,
	int elementWidth,
	NodeId Id)
{
	ID = Id;
	InitLogging(redezvousUri, Id);
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

	perKeySchedule = schedules;
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