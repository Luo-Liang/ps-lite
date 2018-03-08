#include <ps/PLink.h>
#include <ps/OperatorContext.h>

void PLinkExecutor::ReadiyGraph()
{
	//gloo::transport::Device 
	//I only care about my schedule
	std::string host;
	uint port;
	ParseHostPort(rendezvousString, host, port);
	var pRedisStore = make_shared<gloo::rendezvous::RedisStore>(host, port);

	var attribute = gloo::transport::ibverbs::attr();
	attribute.index = 0;
	attribute.name = std::string(pHub->machineConfig.ib_device_names[0]);
	attribute.port = pHub->machineConfig.ib_ports[0];
	var pGlooDefaultDevice = gloo::transport::ibverbs::CreateDevice(attribute);

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
			op->Initialize(pctx);
			if (op->Type == OperatorType::GlooCollectiveAlgorithm)
			{
				//use the same algorithm if theparticipants ar ethe same.
				//this step is quite tricky because gloo rendezvous requires sequential initialization
				//TODO: make sure Gloo is modified to allow prefix match.
				shared_ptr<GlooContext> gContext = dynamic_pointer_cast<GlooContext>(pctx);
				std::shared_ptr<gloo::rendezvous::Context> pContext = std::make_shared<gloo::rendezvous::Context>(gContext->Rank, gContext->Size);
				pctx->additionalContext = pContext;
				//attempt to connect to this mesh
				pContext->connectFullMesh(*pRedisStore, pGlooDefaultDevice);
			}
		}
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
	vector<PLinkKey> myKeys; //make a copy
	unordered_map<PLinkKey, int> keyProg;
	for (Cntr i = 0; i < key2Devs.size(); i++)
	{
		var core = key2Devs.at(i);
		if (tid == core)
		{
			myKeys.push_back((PLinkKey)i);
			keyProg[(PLinkKey)i] = 0;
		}
	}

	//scan ready tasks then execute.
	//not really to do with downstream just a linear execution.

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

					var& seq = keyProg[idx];
					if (seq <= currentNodePerKeySchedule.at(idx).size() - 1)
					{
						//only 1 dependency.
						wQs->WorkQueues[idx] = currentNodePerKeySchedule.at(idx).at(seq + 1);
						seq++;
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