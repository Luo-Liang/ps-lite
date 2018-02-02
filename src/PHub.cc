#include "..\include\ps\phub.h"
#include <numa.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include "..\include\ps\Schedule.h"
#include <unordered_set>

PHub::PHub(Schedule operations, string redezvousUri,
	unordered_map<NodeId, string> nodeToIP,
	unordered_map<PLinkKey, size_t>& size,
	NodeId id)
{
	schedule = operations;
	nodeMap = nodeToIP;
	//now connect that.
	ID = id;
	RendezvousUri = redezvousUri;
	pKeyDescs = make_shared<vector<KeyDesc>>();
	keySizes = size;
}

void PHub::InitializeDevice()
{
	//first, figure out devices.
	machineConfig.SocketCount = numa_available();
	if (machineConfig.SocketCount < 0) machineConfig.SocketCount = 1;
	else machineConfig.SocketCount = numa_num_task_nodes();
	auto maxCpu = getenv("PSHUB_DATA_THREAD_COUNT");
	int allowedCPU = 1;
	if (maxCpu != NULL)
	{
		allowedCPU = atoi(maxCpu);
	}
	CHECK(allowedCPU > 0) << "allowed cpu count is <0";

	//how many processors are there?
	//make sure cpus are balanced.
	CHECK(allowedCPU % machineConfig.SocketCount == 0) << "allowed core count does not divide cpu count";
	machineConfig.CoreCount = allowedCPU;
	auto blockedIFStr = getenv("PHUB_BLOCKED_INTERFACE");
	std::unordered_set<string> blockedIFSet;
	if (blockedIFStr != NULL)
	{
		stringstream ss(blockedIFStr);
		while (ss.good())
		{
			string substr;
			getline(ss, substr, ',');
			blockedIFSet.insert(substr);
			LOG(INFO) << "[" << ID << "] Blocked Interface : " << substr;
		}
	}

	//map a core to a particular socket.
	//do not differentiate worker and server
	auto phubCoreAssignmentScheme = getenv("PHUB_CORE_ASSIGNMENT");
	if (phubCoreAssignmentScheme == NULL)
	{
		phubCoreAssignmentScheme = "sequential";
	}
	machineConfig.Socket2Core.resize(machineConfig.SocketCount);
	for (int i = 0; i < machineConfig.CoreCount; i++)
	{
		if (strcmp(phubCoreAssignmentScheme, "sequential") == 0)
		{
			machineConfig.Core2SocketIdx.push_back(machineConfig.SocketCount * i / machineConfig.CoreCount);
			machineConfig.Socket2Core.at(machineConfig.SocketCount * i / machineConfig.CoreCount).push_back(i);

		}
		else if (strcmp(phubCoreAssignmentScheme, "roundrobin") == 0)
		{
			machineConfig.Core2SocketIdx.push_back(i %  machineConfig.SocketCount);
			machineConfig.Socket2Core.at(i %  machineConfig.SocketCount).push_back(i);
		}
		else
		{
			CHECK(false) << phubCoreAssignmentScheme << " is not a recognized scheme";
		}
	}
	machineConfig.ib_devices = ibv_get_device_list(&machineConfig.ib_num_devices);
	if (machineConfig.ib_devices != NULL)
	{
		//initialize IB devices only if needed.
		auto phubInterfaceAssignmentScheme = getenv("PHUB_IF_ASSIGNMENT");
		if (phubInterfaceAssignmentScheme == NULL)
		{
			phubInterfaceAssignmentScheme = "sequential";
		}
		CHECK(machineConfig.ib_num_devices <= 10) << "More than 10 devices found.";
		//CHECK(machineConfig.ib_num_devices % machineConfig.SocketCount == 0) << "Unbalanced interface count in different sockets.";
		//machineConfig.ib_Socket2DeviceIdx.resize(machineConfig.SocketCount);

		for (uint32_t i = 0; i < machineConfig.ib_num_devices; i++)
		{
			auto ctx = ibv_open_device(machineConfig.ib_devices[i]);
			auto dev = machineConfig.ib_devices[i];
			string nameStr(ibv_get_device_name(dev));
			CHECK(ctx) << "failed to get context for device " << i << " " << nameStr;
			auto idx = nameStr.back() - '0';
			CHECK(idx >= 0 && idx <= 9) << " this card (" << nameStr << ") has a name that does not conform to the format we understand.";
			auto socketId = 0;
			if (strcmp(phubInterfaceAssignmentScheme, "sequential") == 0)
			{
				socketId = idx * machineConfig.SocketCount / machineConfig.ib_num_devices;
			}
			else if (strcmp(phubInterfaceAssignmentScheme, "roundrobin") == 0)
			{
				socketId = idx % machineConfig.SocketCount;
			}
			else
			{
				CHECK(false) << phubInterfaceAssignmentScheme << " is not a recognized scheme";
			}
			machineConfig.ib_Device2SocketIdx.push_back(socketId);
			ibv_device_attr dev_attr;
			CHECK(ibv_query_device(ctx, &dev_attr)) << " error getting device attributes";
			bool breakDueToDesiredDeviceFound = false;
			auto desiredOption = getenv("PHUB_IB_PREFERRED_INTERFACE");
			auto desiredOptionStr = string(desiredOption == NULL ? "" : desiredOption);
			//mlx4_1@1
			CHECK(desiredOptionStr.length() == 0 || desiredOptionStr.end()[-2] == '@') << " name@port is the format";
			auto desiredDevPort = desiredOptionStr.end()[-1] - '0';
			auto desiredDevIF = desiredOptionStr.substr(0, desiredOptionStr.length() - 2);
			for (int p = 1; p < dev_attr.phys_port_cnt; p++)
			{
				ibv_port_attr port_attribute;
				CHECK(ibv_query_port(ctx, p, &port_attribute)) << " error getting port attributes";
				MigrateToNumaNode(socketId);
				auto protection_domain = ibv_alloc_pd(ctx);
				CHECK(protection_domain) << " error getting protection domain " << nameStr << " p=" << p;
				if (port_attribute.link_layer != IBV_LINK_LAYER_INFINIBAND || port_attribute.state != IBV_PORT_ACTIVE)
				{
					continue;
				}
				//if no preference is specified, or a desired device and its port is found
				//also, if it is not blocked 
				if ((desiredDevIF.length() == 0 ||
					(desiredDevIF == nameStr && desiredDevPort == p)) &&
					blockedIFSet.find(nameStr) == blockedIFSet.end())
				{
					//include this interface.
					machineConfig.ib_device_names.push_back(nameStr);
					machineConfig.ib_device_guids.push_back(ntohll(ibv_get_device_guid(machineConfig.ib_devices[i])));
					machineConfig.ib_devices_attribute.push_back(dev_attr);
					machineConfig.ib_protection_domains.push_back(protection_domain);
					machineConfig.ib_contexts.push_back(ctx);
					machineConfig.ib_ports_attribute.push_back(port_attribute);
					machineConfig.ib_ports.push_back(p);
					machineConfig.ib_virtual_devices.push_back(machineConfig.ib_devices[i]);
					machineConfig.ib_Device2SocketIdx.push_back(socketId);

					if (desiredDevIF == nameStr && desiredDevPort == p)
					{
						breakDueToDesiredDeviceFound = true;
						break;
					}
				}
			}
			if (breakDueToDesiredDeviceFound)
			{
				break;
			}
		}
		//the direct connect approach is deprecated
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_contexts.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_device_names.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_virtual_devices.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_device_guids.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_devices_attribute.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_ports_attribute.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_ports.size());
		CHECK(machineConfig.ib_protection_domains.size() == machineConfig.ib_Device2SocketIdx.size());
	}
	machineConfig.Initialized = true;
	phubRendezvous->PushMachineConfig(ID, machineConfig);
	//add more code to support non-IB cards.
	//not for now
}

//i only care about nodes i communicate with
void PHub::InitializePHubSpecifics()
{
	//use 1 QP per connection.
	//how many remotes are there?
	//??????????
	//who am i talking to?
	vector<MachineConfigDescSlim> descs;
	//unordered_map<NodeId, vector<vector<int>>> remoteSockQPIdxs;
	unordered_map<int, NodeId> qp2RemoteIdx;
	unordered_map<int, int> qp2RemoteDevIdx;
	int totalQPCnt = 0;

	for (var item : nodeMap)
	{
		if (ID != item.first)
		{
			var mcfg = phubRendezvous->PullMachineConfig(item.first);
			//var& vec = remoteSockQPIdxs[item.first];
			//vec.resize(mcfg.NumSockets);
			for (Cntr card = 0; card < mcfg.Devices2Socket.size(); card++)
			{
				//one qp connection per card.
				qp2RemoteIdx[totalQPCnt] = item.first;
				qp2RemoteDevIdx[totalQPCnt] = card;
				//vec.at(mcfg.Devices2Socket.at(card)).push_back(totalQPCnt);
				totalQPCnt++;
				CHECK(qp2RemoteIdx.size() == totalQPCnt);
			}
		}
	}

	//now i need to distribute keys equally to these QPs.
	vector<PLinkKey> keys;
	vector<float> kSizes;
	for (var item : keySizes)
	{
		keys.push_back(item.first);
		kSizes.push_back((float)item.second);
	}
	vector<int> key2QP = approximateSetPartition(kSizes, totalQPCnt);
	//there maybe more QP than there are devices on local machine.
	//assign QP to devices.
	vector<float> qpPayloadSizes;
	qpPayloadSizes.resize(totalQPCnt);
	for (Cntr i = 0; i < key2QP.size(); i++)
	{
		var key = keys.at(i);
		var qp = key2QP.at(i);
		qpPayloadSizes.at(qp) += keySizes.at(key);
	}

	//assign qp to devices.
	vector<int> qp2Dev = approximateSetPartition(qpPayloadSizes, machineConfig.ib_device_names.size());
	CHECK(qp2Dev.size() == totalQPCnt);

	//assign qp to cores.

	//how to partition qps to cq?
	//balance cq load.
	//load balance qp to cq (core).

	//there may be fewer cores than interfaces.
	//the minimum number of interfaces is the number of cards involved and the number of created q pairs.
	vector<int> qp2cq;
	int cqCnt = min(totalQPCnt, machineConfig.ib_num_devices);

	//create one CQ per device.
	for (Cntr i = 0; i < totalQPCnt; i++)
	{
		var dev = qp2Dev.at(i);
		qp2cq.push_back(dev);
	}

	//map cq to cores.
	vector<int> cq2Cores;
	cq2Cores.resize(cqCnt);
	vector<int> socketTicketer(machineConfig.SocketCount);
	for (Cntr i = 0; i < cqCnt; i++)
	{
		//what's my socket?
		//just device i.
		var socket = machineConfig.ib_Device2SocketIdx.at(i);
		int& currentIdx = socketTicketer.at(socket);
		var core = machineConfig.Socket2Core.at(socket).at(currentIdx);
		currentIdx++;
		cq2Cores.at(i) = core;
	}

	//we now have a mapping of how qps map to devices and cores.
	//we need to broadcast this information.
	//first, create CQs.
	const var CQDepth = 8192;
	const var SGElement = 2;
	const var MaxInlineData = 16;
	for (Cntr i = 0; i < cqCnt; i++)
	{
		var dev = i;
		var pSCQ = ibv_create_cq(machineConfig.ib_contexts.at(dev), CQDepth, NULL, NULL, 0);
		CHECK(pSCQ != NULL);
		SCQs.push_back(pSCQ);
		var pRCQ = ibv_create_cq(machineConfig.ib_contexts.at(dev), CQDepth, NULL, NULL, 0);
		CHECK(pRCQ != NULL);
		RCQs.push_back(pRCQ);
	}


	//first, create these queue pairs.
	for (Cntr i = 0; i < totalQPCnt; i++)
	{
		var dev = qp2Dev.at(i);
		var pPD = machineConfig.ib_protection_domains.at(dev);
		ibv_qp_init_attr init_attributes;
		std::memset(&init_attributes, 0, sizeof(ibv_qp_init_attr));
		var cqIdx = qp2cq.at(i);
		init_attributes.send_cq = SCQs.at(cqIdx);
		init_attributes.recv_cq = RCQs.at(cqIdx);
		CHECK(cqIdx == dev) << "Cannot create cq on a wrong device";
		init_attributes.qp_type = IBV_QPT_RC;
		//no signal
		init_attributes.sq_sig_all = 0;
		init_attributes.cap.max_send_wr = CQDepth;
		init_attributes.cap.max_recv_wr = CQDepth;
		init_attributes.cap.max_send_sge = SGElement;
		init_attributes.cap.max_recv_sge = SGElement;
		init_attributes.cap.max_inline_data = MaxInlineData;
		//now create queue pairs.
		var pQP = ibv_create_qp(pPD, &init_attributes);
		CHECK(pQP != NULL);
		QPs.push_back(pQP);
	}


	//foreach device, broadcast the map to different endpoint's different devices.
	for (Cntr i = 0; i < machineConfig.ib_num_devices; i++)
	{
		//create a dictionary for broadcast.
		var myKey = CxxxxStringFormat("%d:%d", ID, i);
		unordered_map<string, int> bcast;
		//pick out my talks.
		for (Cntr id = 0; id < totalQPCnt; id++)
		{
			var qpRemote = qp2RemoteIdx.at(id);
			var qpDevId = qp2RemoteDevIdx.at(id);
			if (qpDevId == i)
			{
				var remoteName = CxxxxStringFormat("%d:%d", qpRemote, qpDevId);
				bcast[remoteName] = QPs.at(id)->qp_num;
			}
		}
		phubRendezvous->PullQP();
		//foreach remote device, tell me what is my queue pair number?
	}
}

void PHub::InitializeDeviceSpecifics()
{
	CHECK(machineConfig.Initialized);
	auto redisVec = CxxxxStringSplit(RendezvousUri, ':');
	CHECK(redisVec.size() == 2);
	var redisAddr = redisVec[0];
	var redisPort = atoi(redisVec[1].c_str());
	CHECK(redisPort != -1);
	//this is the place to initialize device specific structures, such as ...
	//queue pairs.
	//use redis for rendezvous
	//figure out from schedule who i need to get in touch with
	pRedisStore = make_shared<gloo::rendezvous::RedisStore>(redisAddr, redisPort);
	CHECK(machineConfig.ib_device_names.size() > 0);
	var attribute = gloo::transport::ibverbs::attr();
	attribute.index = 0;
	attribute.name = std::string(machineConfig.ib_device_names[0]);
	attribute.port = machineConfig.ib_ports[0];
	pGlooDefaultDevice = gloo::transport::ibverbs::CreateDevice(attribute);
	phubRendezvous = make_shared<Rendezvous>(redisAddr, redisPort);
	phubRendezvous->Connect();
	//gloo::transport::Device 
	//I only care about my schedule
	var mySchedule = schedule.Filter(ID);
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
		phubRendezvous->SynchronousBarrier(op->GetUniqueName(), nodeMap.size());
	}
	//use 1 queue pair for a remote interface.

}

int PHub::Push(NodeId destination, BufferHandle buf)
{
	return 0;
}
