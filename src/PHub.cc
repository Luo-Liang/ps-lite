#include "..\include\ps\phub.h"
#include <numa.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include "..\include\ps\Schedule.h"
#include <unordered_set>

PHub::PHub(Schedule operations,
	string redezvousUri,
	unordered_map<NodeId, string> nodeToIP,
	vector<float>& sizes,
	int totalParticipant,
	int elementWidth,
	NodeId id)
{
	totalPHubNodes = totalParticipant;
	ElementWidth = elementWidth;
	schedule = operations;
	nodeMap = nodeToIP;
	//now connect that.
	ID = id;
	RendezvousUri = redezvousUri;
	pKeyDescs = make_shared<vector<KeyDesc>>();
	keySizes = sizes;
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
	//who am i talking to?
	unordered_map<NodeId, MachineConfigDescSlim> descs;
	//unordered_map<NodeId, vector<vector<int>>> remoteSockQPIdxs;
	vector<NodeId> qp2RemoteNode;
	vector<int> qp2RemoteDevIdx;
	int totalQPCnt = 0;

	//first, assign keys to devices.
	key2Dev = approximateSetPartition(keySizes, machineConfig.ib_num_devices);

	//now i need to distribute keys equally to these QPs.
	//keys should be contiguous.
	vector<float> qpPayloadSizes;
	//given a remote and a key, tell me which qp i should data to.
	//pull device information from all remotes.
	var totalRemoteDevs = 0;
	for (var item : nodeMap)
	{
		if (ID != item.first)
		{
			var mcfg = phubRendezvous->PullMachineConfig(item.first);
			descs[item.first] = mcfg;
			totalRemoteDevs += mcfg.Devices2Socket.size();
		}
	}

	//dev to keys.
	vector<vector<PLinkKey>> dev2Keys(machineConfig.ib_num_devices);
	for (Cntr i = 0; i < key2Dev.size(); i++)
	{
		var dev = key2Dev.at(i);
		dev2Keys.at(dev).push_back(i);
	}

	vector<int> qp2Dev;
	unordered_map<string, int> qpTicketer;
	for (var remotes : nodeMap)
	{
		if (remotes.first == ID) continue;
		var& cfg = descs.at(remotes.first);
		var remoteKey2Dev = approximateSetPartition(keySizes, cfg.Devices2Socket.size());

		//foreach local card, figure out who to connect?
		for (Cntr i = 0; i < machineConfig.ib_num_devices; i++)
		{
			var& myKeys = dev2Keys.at(i);
			//whoever on the remote that is in charge of the keys require connection.
			remoteKey2QPIdx.at(remotes.first).resize(keySizes.size());
			for (int kIdx : myKeys)
			{
				var rDev = remoteKey2Dev.at(kIdx);
				var rName = CxxxxStringFormat("LOCAL:%d:%d:%d", i, remotes.first, rDev);
				//grab a ticket.
				var existingQP = qpTicketer.find(rName);
				if (existingQP == qpTicketer.end())
				{
					//first time seen.
					var id = qpTicketer.size();
					qp2Dev.push_back(i);
					qp2RemoteNode.push_back(remotes.first);
					qp2RemoteDevIdx.push_back(rDev);
					remoteKey2QPIdx.at(remotes.first).at(kIdx) = id;
					CHECK(qp2Dev.size() == qpTicketer.size());
				}
				else
				{
					//this qp already exists, just getting assigned a new key.
					var id = existingQP->second;
					//no qp is created, however, we need to assign remoteKey2QPIdx.
					remoteKey2QPIdx.at(remotes.first).at(kIdx) = id;
				}
			}
		}
	}

	totalQPCnt = qp2RemoteNode.size();
	Endpoints.resize(totalQPCnt);

	//assign qp to devices.
	//vector<int> qp2Dev = approximateSetPartition(qpPayloadSizes, machineConfig.ib_device_names.size());
	//CHECK(qp2Dev.size() == totalQPCnt);

	//assign qp to cores.

	//how to partition qps to cq?
	//balance cq load.
	//load balance qp to cq (core).

	//there may be fewer cores than interfaces.
	//the minimum number of interfaces is the number of cards involved and the number of created q pairs.
	vector<int> qp2cq;
	int cqCnt = min(totalQPCnt, machineConfig.ib_num_devices);

	for (Cntr i = 0; i < totalQPCnt; i++)
	{
		var dev = qp2Dev.at(i);
		//a single CQ is used in a single device.
		qp2cq.push_back(dev);
		Endpoints.at(i).CQIdx = dev;
		Endpoints.at(i).DeviceIdx = dev;
		Endpoints.at(i).SocketIdx = machineConfig.ib_Device2SocketIdx.at(dev);
	}

	//map cq to cores.
	vector<int> cq2Cores(cqCnt);
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

		//at this stage, Endpoint should have core assigned.
		Endpoints.at(i).CoreIdx = cq2Cores.at(Endpoints.at(i).CQIdx);
		CHECK(Endpoints.at(i).SocketIdx == machineConfig.Core2SocketIdx.at(Endpoints.at(i).CoreIdx));
		Endpoints.at(i).Index = i;
		Endpoints.at(i).RemoteMachineIdx = qp2RemoteNode.at(i);
		Endpoints.at(i).RemoteCardIdx = qp2RemoteDevIdx.at(i);
		Endpoints.at(i).LocalQPNum = pQP->qp_num;
		Endpoints.at(i).LocalLid = machineConfig.ib_ports_attribute.at(dev).lid;
	}


	//foreach device, broadcast the map to different endpoint's different devices.
	for (Cntr i = 0; i < machineConfig.ib_num_devices; i++)
	{
		//create a dictionary for broadcast.
		var myKey = CxxxxStringFormat("%d:%d", ID, i);
		unordered_map<string, int> bcast;
		//pick out my talks.
		bcast["lid"] = machineConfig.ib_ports_attribute.at(i).lid;
		for (Cntr id = 0; id < totalQPCnt; id++)
		{
			var qpRemote = qp2RemoteNode.at(id);
			var qpDevId = qp2Dev.at(id);
			var qpRemoteDevId = qp2RemoteDevIdx.at(id);
			if (qpDevId == i)
			{
				//this is my QP, talking to qpRemote:qpRemoteDevId
				var remoteName = CxxxxStringFormat("%d:%d", qpRemote, qpRemoteDevId);
				bcast[remoteName] = QPs.at(id)->qp_num;
			}
		}
		var mapName = CxxxxStringFormat("QPAndLidExchange:%d:%d", ID, i);
		phubRendezvous->PushMap<int>(mapName, bcast);
		//foreach remote device, tell me what is my queue pair number?
	}

	//rendezvous
	phubRendezvous->SynchronousBarrier("qp_exchange", totalPHubNodes);

	//pull my connections.
	//my connections include the stuff in the node map.
	unordered_map<string, unordered_map<string, int>> cardQPCache;
	for (var p : nodeMap)
	{
		var rNode = p.first;
		//how many devices in this?
		var mcfg = descs[rNode];
		var totalDevs = mcfg.Devices2Socket.size();

		//pull queue pairs.
		for (Cntr i = 0; i < totalDevs; i++)
		{
			var key = CxxxxStringFormat("QPAndLidExchange:%d:%d", rNode, i);
			cardQPCache[key] = phubRendezvous->PullMap<int>(key);
		}
	}

	//now assign qp to current qps.
	for (Cntr i = 0; i < totalQPCnt; i++)
	{
		var rNode = qp2RemoteNode.at(i);
		var rCard = qp2RemoteDevIdx.at(i);

		var rName = CxxxxStringFormat("%d:%d", rNode, rCard);

		var mNode = ID;
		var mCard = qp2Dev.at(i);

		var mName = CxxxxStringFormat("%d:%d", mNode, mCard);

		var& map = cardQPCache.at(rName);

		Endpoints.at(i).RemoteLid = map.at("lid");
		Endpoints.at(i).RemoteQPNum = map.at(mName);
	}

	//check integrity.
	//nodeMap.size includes me.
	vector<int> key2Sock(keySizes.size());
	for (Cntr i = 0; i < keySizes.size(); i++)
	{
		var dev = key2Dev.at(i);
		var sock = machineConfig.ib_Device2SocketIdx.at(dev);
		key2Sock.at(i) = sock;
	}
	allocator.Init(keySizes, nodeMap.size(), CxxxxSelect<int, int>(key2Dev, [key2Sock](int x) { return key2Sock.at(x); }), ElementWidth);
	var mrs = allocator.ReadyRDMA(machineConfig.ib_protection_domains, machineConfig.ib_Device2SocketIdx);
	phubRendezvous->SynchronousBarrier("PHubAllocator", totalPHubNodes);

	//first, create a node to index mapping
	int index = 0;
	for (var remote : nodeMap)
	{
		if (remote.first == ID) continue;
		//populate remote to idx.
		nodeID2Index.at(remote.first) = index;
		unordered_map<string, uint64_t> addrMap;
		for (Cntr i = 0; i < keySizes.size(); i++)
		{
			var sock = key2Sock.at(i);
			//create a broadcast map.
			size_t notUsed;
			var name = CxxxxStringFormat("KEY:%d", i);
			addrMap[name] = (uint64_t)allocator.PHUBReceiveKVBuffer(i, index, sock, notUsed);
			var rKeyName = CxxxxStringFormat("RKEY:%d", i);
			var dev = key2Dev.at(i);
			addrMap[rKeyName] = mrs.at(i)->rkey;
		}
		index++;
		var mapName = CxxxxStringFormat("ADDR:%d:%d", ID, remote.first);
		phubRendezvous->PushMap<uint64_t>(mapName, addrMap);
	}

	//barrier.
	phubRendezvous->SynchronousBarrier("AddressExchange", totalPHubNodes);

	//now, pull addresses.
	vector<unordered_map<std::string, uint64_t>> remote2AddrMap(nodeID2Index.size());
	for (var remote : nodeMap)
	{
		if (remote.first == ID) continue;
		//someone to me.
		var targetName = CxxxxStringFormat("ADDR:%d:%d", remote.first, ID);
		remote2AddrMap.at(nodeID2Index.at(remote.first)) = phubRendezvous->PullMap<uint64_t>(targetName);
	}

	for (Cntr i = 0; i < keySizes.size(); i++)
	{
		var sock = key2Sock.at(i);
		size_t notUsed;
		var mBuffer1 = allocator.PHUBMergeKVBuffer(i, sock, 0, notUsed);
		var mBuffer2 = allocator.PHUBMergeKVBuffer(i, sock, 1, notUsed);
		//get address from all remotes.
		vector<uint64_t> rdmaRemoteAddrs;
		vector<int> remoteKeys;
		for (Cntr node = 0; node < nodeID2Index.size(); node++)
		{
			var name = CxxxxStringFormat("KEY:%d", i);
			rdmaRemoteAddrs.push_back(remote2AddrMap.at(node).at(name));
			//how do i grab remote keys?
			var rKeyName = CxxxxStringFormat("RKEY:%d", i);
			remoteKeys.push_back(remote2AddrMap.at(node).at(rKeyName));
		}
		MergeBuffers.at(i).Init(keySizes.at(i), i, (char*)mBuffer1, (char*)mBuffer2, rdmaRemoteAddrs, remoteKeys, ID, ElementWidth);
	}
	//copy data from init address to merge buffers.
	for (Cntr i = 0; i < ApplicationSuppliedAddrs.size(); i++)
	{
		var addr = MergeBuffers.at(i).GetCurrentReadBuffer();
		memcpy(addr, ApplicationSuppliedAddrs.at(i), keySizes.at(i));
	}
	//initialize QP counters
	QPStats.resize(totalQPCnt, CQDepth - 1);
	//now, connect the queue pairs.
	for (size_t i = 0; i < Endpoints.size(); ++i) {
		auto devId = Endpoints.at(i).DeviceIdx;
		CHECK(QPs.at(i) != NULL);
		ibv_qp_attr attributes;
		std::memset(&attributes, 0, sizeof(attributes));

		// move to INIT
		attributes.qp_state = IBV_QPS_INIT;
		attributes.port_num = machineConfig.ib_ports.at(devId);
		attributes.pkey_index = 0;
		attributes.qp_access_flags =
			(IBV_ACCESS_LOCAL_WRITE |
				IBV_ACCESS_REMOTE_WRITE |
				IBV_ACCESS_REMOTE_READ |
				IBV_ACCESS_REMOTE_ATOMIC);
		int retval = ibv_modify_qp(QPs[i], &attributes,
			IBV_QP_STATE |
			IBV_QP_PKEY_INDEX |
			IBV_QP_PORT |
			IBV_QP_ACCESS_FLAGS);
		if (retval < 0) {
			printf("[%d]ibv_modify_qp[%d] phase1 retVal=%d\n", ID, i, retval);
			perror("Error setting queue pair to INIT");
			exit(1);
		}


		/// in theory, we need to post an empty receive WR to proceed, but
		/// when we're doing RDMA-only stuff it seems to work without one.

		// move to RTR
		std::memset(&attributes, 0, sizeof(attributes));
		attributes.qp_state = IBV_QPS_RTR;
		attributes.path_mtu = machineConfig.ib_ports_attribute.at(devId).active_mtu;
		attributes.dest_qp_num = Endpoints[i].RemoteQPNum;
		attributes.rq_psn = 0;
		attributes.max_dest_rd_atomic = max_dest_rd_atomic;
		attributes.min_rnr_timer = min_rnr_timer;
		attributes.ah_attr.is_global = 0;
		attributes.ah_attr.dlid = Endpoints[i].RemoteLid;
		attributes.ah_attr.sl = 0;
		attributes.ah_attr.src_path_bits = 0;
		attributes.ah_attr.port_num = machineConfig.ib_ports.at(devId);
		retval = ibv_modify_qp(QPs.at(i), &attributes,
			IBV_QP_STATE |
			IBV_QP_AV |
			IBV_QP_PATH_MTU |
			IBV_QP_DEST_QPN |
			IBV_QP_RQ_PSN |
			IBV_QP_MAX_DEST_RD_ATOMIC |
			IBV_QP_MIN_RNR_TIMER);
		if (retval < 0) {
			perror("Error setting queue pair to RTR");
			exit(1);
		}
		//printf("[%d]ibv_modify_qp[%d] phase2\n", thisVan->my_node().id, i);
		// move to RTS
		std::memset(&attributes, 0, sizeof(attributes));
		attributes.qp_state = IBV_QPS_RTS;
		attributes.timeout = timeout;
		attributes.retry_cnt = retry_count;
		attributes.rnr_retry = rnr_retry;
		attributes.sq_psn = 0;
		attributes.max_rd_atomic = max_rd_atomic;
		retval = ibv_modify_qp(QPs.at(i), &attributes,
			IBV_QP_STATE |
			IBV_QP_TIMEOUT |
			IBV_QP_RETRY_CNT |
			IBV_QP_RNR_RETRY |
			IBV_QP_SQ_PSN |
			IBV_QP_MAX_QP_RD_ATOMIC);
		if (retval < 0) {
			perror("Error setting queue pair to RTR");
			exit(1);
		}
	}
	//clear ready bits
	ReadyBit.resize(nodeID2Index.size(), vector<bool>(keySizes.size(), false));

	//post receive buffers.
	//i should receive all keys from all workers.
	//except me.
	//this is the safest
	ReceiveRequests.resize(nodeID2Index.size());
	for (var remote : nodeID2Index)
	{
		var idx = remote.second;
		ReceiveRequests.at(idx).resize(keySizes.size());
		for (Cntr j = 0; j < keySizes.size(); j++)
		{
			memset(&ReceiveRequests.at(idx).at(j).ReceiveRequest, NULL, sizeof(ibv_recv_wr));
			ReceiveRequests.at(idx).at(j).QPIndex = remoteKey2QPIdx.at(remote.first).at(j);
			//ReceiveRequests.at(idx).at(j).ReceiveRequest.
			PostReceiveRequest(ReceiveRequests.at(idx).at(j));
		}
	}
	//finalize
	phubRendezvous->SynchronousBarrier("PostReceiveWorkItems", totalPHubNodes);
}

inline void PHub::PostReceiveRequest(IBReceiveRequest& req)
{
	//because you chose send signaled.
	PostReceive(req.QPIndex, &req.ReceiveRequest);
}

inline std::string PHub::GetWRSummary(ibv_send_wr* wr)
{
	std::stringstream ss;
	if (wr == NULL)
	{
		return "";
	}
	else
	{
		ss << "wr->op = " << wr->opcode << ",->key = " << (PHUB_MAX_KEY & wr->imm_data) << ",->next = " << wr->next << " .rdma.addr = " << wr->wr.rdma.remote_addr << " .rkey = " << wr->wr.rdma.rkey;
		for (size_t i = 0; i < wr->num_sge; i++)
		{
			ss << ", wr->sge[" << i << "].length=" << wr->sg_list[i].length << " .lkey=" << wr->sg_list[i].lkey << " .addr=" << wr->sg_list[i].addr;
		}
		ss << std::endl;
		ss << "...additional wr: " << GetWRSummary(wr->next);
	}
	return ss.str();
}

void PHub::PostSend(int index, ibv_send_wr * wr) {
	ibv_send_wr * bad_wr = nullptr;

	//printf("[%d] attempting to post send wr to endpoint %d ... %s\n", myId, QPIdx, GetWRSummary(wr).c_str());

	int retval = ibv_post_send(QPs.at(index), wr, &bad_wr);
	if (retval != 0) {
		printf("[%d] Error posting send wr to endpoint %d failed. wr.op = %d, .next = %p, .key = %d, .dest = %d, %s, Stack = %s\n", ID, index, wr->opcode, wr->next, PHUB_MAX_KEY & wr->imm_data, (wr->imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK, GetWRSummary(wr).c_str(), GetStacktraceString().c_str());
		if (wr->next != NULL)
		{
			printf("[%d] Cont' .next.next = %p, .next.op = %d\n", myId, wr->next->next, wr->next->opcode);
		}
		perror("Error posting send WR");

		if (bad_wr) {
			std::cerr << "Error posting send WR at WR " << wr << " (first WR in list was " << bad_wr << ")" << std::endl;
			printf("[%d] Badwr @ endpoint %d ... badwr.op = %d, .next = %p, .key = %d, .dest = %d. wr summary = %s\n", ID, index, bad_wr->opcode, bad_wr->next, PHUB_MAX_KEY & bad_wr->imm_data, (bad_wr->imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK, GetWRSummary(bad_wr).c_str());
		}

		exit(1);
	}
}

inline bool PHub::UpdateQPCounter(size_t qpIdx, int dec)
{
	QPStats[qpIdx] -= dec;// dec;
						  //printf("[%d]QPStats[%d] = %d, deced = %d\n", myId, (int)qpIdx, (int)QPStats[qpIdx], dec);
						  //the two last messages need to do this.
	if (QPStats[qpIdx] <= 0)
	{
		QPStats[qpIdx] = CQDepth - 1;
		return true;
	}
	else
	{
		return false;
	}
}

int PHub::Poll(int max_entries, int CQIndex, CompletionQueueType type, ibv_wc* wc) {
	CHECK(CQIndex < SCQs.size() && CQIndex >= 0);
	int retval = -1;
	if (type == CompletionQueueType::Send)
		retval = ibv_poll_cq(SCQs[CQIndex], max_entries, wc);
	else if (type == CompletionQueueType::Receive)
		retval = ibv_poll_cq(RCQs[CQIndex], max_entries, wc);
	if (retval < 0) {
		std::cerr << "Failed polling completion queue with status " << retval << "\n";
		exit(1);
	}
	else if (retval > 0) {
		if (wc->status == IBV_WC_SUCCESS) {
			if (wc->opcode == IBV_WC_RDMA_WRITE) {
#ifdef VERBOSE
				std::cout << "Got completion for WR ID " << wc.wr_id << std::endl;
#endif
			}
			else if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
#ifdef VERBOSE
				std::cout << "Got completion for WR ID " << wc.wr_id << " with immediate value " << (void*)((int64_t)wc.imm_data) << std::endl;
#endif
			}
			else {
#ifdef VERBOSE
				std::cout << "Got completion for something with id " << ((int64_t)wc.wr_id) << std::endl;
#endif
			}
			}
		else {
			printf("[%d] polling Qidx=%d IsSendQ=%d got status %s. SendCompletionQueue.size() = %d RecvCompletionQueue.size() = %d,  StackTrace=%s, wc->wr_id = %d\n",
				ID,
				CQIndex,
				type == CompletionQueueType::Send,
				ibv_wc_status_str(wc->status),
				SCQs.size(),
				RCQs.size(),
				GetStacktraceString().c_str(),
				wc->wr_id);
			CHECK(false) << " Details to follow: ID = "
				<< ID
				<< " Send="
				<< (type == CompletionQueueType::Send)
				<< " Idx="
				<< CQIndex
				<< " error= "
				<< strerror(errno);
		}
			}
	return retval;
}

void PHub::VerbsSmartPost(int QPIndex, ibv_send_wr* wr)
{
	//post_send uses a spinlock so it's fine to lock for it here.
	//be aware:: pshub servers do not require locking.
	wr->wr_id = QPIndex;
	//if true is removed we are pulling more than expected things out.
	//no metadata is requested.
	if (UpdateQPCounter(QPIndex, 1))
	{
		CHECK(wr->next == NULL);
		wr->send_flags = IBV_SEND_SIGNALED;
		//we need to poll then send.
		//printf("[%d] sending out qpindex = %d, polling cqIdx = %d, wr = %s. ep = %s\n", myId, QPIndex, cqIndex, GetWRSummary(wr).c_str(), GetEndpointSummary(QPIndex).c_str());

		PostSend(QPIndex, wr);
		//at most 2.
		ibv_wc wc;
		while (0 == Poll(1, Endpoints.at(QPIndex).CQIdx, CompletionQueueType::Send, &wc));
	}
	else
	{
		wr->send_flags = 0;
		CHECK(wr->next == NULL);
		//just send
		PostSend(QPIndex, wr);
	}
}


void PHub::Push(PLinkKey key, NodeId destination)
{
	//now as if everything is correct.
	var qpIdx = remoteKey2QPIdx.at(destination).at(key);
	///For non-accurate optimizations, make sure to check the first Sizeof(metaSlim) buffer is not touched.
	auto& mergeBuffer = MergeBuffers.at(key);
	auto buffer = MergeBuffers.at(key).GetCurrentReadBuffer();
	auto destinationIdx = nodeID2Index.at(destination);
	//o is meta, 1 is kv. 
	mergeBuffer.RDMAWorkerSendSgesArray.at(destinationIdx).addr = (uint64_t)buffer;
	auto& ep = Endpoints.at(qpIdx);
	var CQIndex = ep.CQIdx;
	auto lkey = allocator.MemoryRegions.at(ep.SocketIdx)->lkey;
	//use the correct lkeys.
	//RDMAWorkerSendMetaSges.at(remoteMachine).lkey = RDMAWorkerSendKVSges[remoteMachine].lkey = lkey;
	//just in case.
	mergeBuffer.RDMAWorkerSendSgesArray.at(destinationIdx).lkey = lkey;
	VerbsSmartPost(qpIdx, &mergeBuffer.RDMARemoteSendKVRequests.at(destinationIdx));
	//determine if you'd like to poll to ensure push is done?
}

inline void PHub::PostReceive(int QPIdx, ibv_recv_wr * wr) {
	ibv_recv_wr * bad_wr = nullptr;
	int retval = ibv_post_recv(QPs.at(QPIdx), wr, &bad_wr);
	if (retval != 0) {
		//        char buf[500];
		//print_stacktrace();
		printf("[%d]Error posting receive WR to endpoint %d. errno = %d. More informatin follows. Stack = %s\n", ID, QPIdx, retval, GetStacktraceString().c_str());
		perror("Error posting receive");
		exit(1);
	}
	if (bad_wr) {
		printf("[%d]Error posting receive WR. BadWR = %p, CurrentWR= %p\n", ps::Postoffice::Get()->van()->my_node().id, bad_wr, wr);
		exit(1);

	}

	//printf("[%d][success] attempting to post receive wr to endpoint %d\n", myId, QPIdx);
}
void PHub::PullOnce(PLinkKey pkey, NodeId source)
{
	//first, check to see if someone already pulled this for me?
	if (ReadyBit.at(source).at(pkey) == true)
	{
		ReadyBit.at(source).at(pkey) = false;
		return;
	}
}
void PHub::Pull(PLinkKey pkey, NodeId source)
{
	//who is going to actually poll this??
	//any thread can, but it needs to be handled by the dedicated phub thread.
	//or a streamlined plink system thread should pull it??
	///let's try with the latter first?
	//this thread is indeed in charge of the key

	//first, check to see if someone already pulled this for me?
	if (ReadyBit.at(source).at(pkey) == true)
	{
		ReadyBit.at(source).at(pkey) = false;
		return;
	}
	//which cq does this key belong to?
	var cqIdx = key2Dev.at(pkey);
	const var copies = 5;
	//i expect many keys to be ready at the same time.
	//poll them out 
	ibv_wc wcs[copies];
	bool found = false;
	while (found == false)
	{
		var returned = Poll(copies, cqIdx, CompletionQueueType::Receive, wcs);
		//mark everyone as ready.
		for (Cntr i = 0; i < returned; i++)
		{
			var sender = GET_SENDER_FROM_IMM(wcs[i].imm_data);
			var key = GET_KEY_FROM_IMM(wcs[i].imm_data);
			ReadyBit.at(sender).at(key) = true;
		}
		found = ReadyBit.at(source).at(pkey);
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
		phubRendezvous->SynchronousBarrier(op->GetUniqueName(), totalPHubNodes);

	}
	//use 1 queue pair for a remote interface.

}
