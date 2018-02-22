#pragma once


#include "PLink.h"

#define MAX_PHUB_NODES 1000

struct KeyDesc
{
	PLinkKey Key;
	EndPoint EP;
};
//represent one connection of two infiniband or IP nodes
struct EndPoint
{
	uint16_t lid = -1;        // InfiniBand address of node. Remote Lid
	uint32_t qp_num = -1;     // Queue pair number on node (like IP port number)
	ibv_qp * queue_pair = NULL;
	int DeviceIdx = -1;
	int SocketIdx = -1;
	int RemoteMachineIdx = -1;
	int CoreIdx = -1;
	int RemoteQPIdx = -1;
	int CQIdx = -1;
	int Index = -1;
	uint32_t RemoteKey = 0;
	uint16_t LocalLid = 0;
};

struct MachineConfigDescriptor
{
	bool Initialized = false;
	ibv_device ** ib_devices = NULL;
	int ib_num_devices = 0;
	std::vector<ibv_device*> ib_virtual_devices;
	std::vector< const char *> ib_device_names;
	std::vector<uint64_t> ib_device_guids;
	std::vector<ibv_device_attr> ib_devices_attribute;
	std::vector<uint8_t> ib_ports;
	std::vector<ibv_context *> ib_contexts;
	std::vector<ibv_pd *> ib_protection_domains;
	std::vector<ibv_port_attr> ib_ports_attribute;
	std::vector<int> ib_Device2SocketIdx;
	//std::vector<int> ib_Socket2DeviceIdx;
	int SocketCount = 0;
	int CoreCount = 0;
	std::vector<int> Core2SocketIdx;
	std::vector<int> Socket2CoreCount;
	std::vector<ibv_mr*> ib_DeviceMemoryRegions;
	std::vector<vector<int>> Socket2Core;


};

struct MachineConfigDescSlim
{
public:
	std::vector<int> Devices2Socket;
	std::vector<int> Cores2Socket;
	int NumSockets;
};

struct PHubEndpoint {
	uint16_t LocalLid = -1;
	uint16_t RemoteLid = -1;        // InfiniBand address of node. Remote Lid
	uint32_t RemoteQPNum = -1;     // Queue pair number on node (like IP port number)
	uint32_t LocalQPNum = -1;
	int DeviceIdx = -1;
	int SocketIdx = -1;
	int RemoteMachineIdx = -1;
	int RemoteCardIdx = -1;
	int CoreIdx = -1;
	int CQIdx = -1;
	int Index = -1;
};

class PHub
{
	enum CompletionQueueType
	{
		Send,
		Receive
	};
	const int CQDepth = 8192;
	const int SGElement = 2;
	const int MaxInlineData = 16;
	const int scatter_gather_element_count = 1; // how many SGE's do we allow per operation?
	const int max_inline_data = 16;             // message rate drops from 6M/s to 4M/s at 29 bytes
	const int max_dest_rd_atomic = 16;          // how many outstanding reads/atomic ops are allowed? (remote end of qp, limited by card)
	const int max_rd_atomic = 16;               // how many outstanding reads/atomic ops are allowed? (local end of qp, limited by card)
	const int min_rnr_timer = 0x12;             // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
	const int timeout = 0x12;                   // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
	const int retry_count = 6;                  // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
	const int rnr_retry = 0;                    // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
	shared_ptr<vector<KeyDesc>> pKeyDescs;
	std::vector<ibv_qp*> QPs;
	std::vector<ibv_cq*> SCQs;
	std::vector<ibv_cq*> RCQs;
	int totalPHubNodes;
	std::vector<PHubEndpoint> Endpoints;
	vector<int> key2Dev;
	unordered_map<PLinkKey, unordered_map<NodeId, int>> KeyToQPIdx;
	PHubAllocator allocator;
	int ElementWidth;
	unordered_map<NodeId, int> nodeID2Index;
	vector<PHubMergeBuffer> MergeBuffers;
	unordered_map<NodeId, vector<int>> remoteKey2QPIdx;
	void VerbsSmartPost(int QPIndex, ibv_send_wr* wr);
	vector<int> QPStats;
	inline bool UpdateQPCounter(size_t qpIdx, int dec);
	void PostSend(int remote_rank, ibv_send_wr * wr);
	inline std::string GetWRSummary(ibv_send_wr* wr);
	inline void PostReceiveRequest(IBReceiveRequest& req);
	int Poll(int max_entries, int QIndex, CompletionQueueType type, ibv_wc* wc);
	inline void PostReceive(int QPIdx, ibv_recv_wr * wr);
	vector<int> UpdatesReceived;
	//remote key ready bit.
	vector<vector<bool>> ReadyBit;
	//remote, key
	std::vector<vector<IBReceiveRequest>> ReceiveRequests;

public:
	//global keysizes assuming contiguous keys.
	//in bytes;
	vector<float> keySizes;
	unordered_map<NodeId, string> nodeMap;
	MachineConfigDescriptor machineConfig;
	//my global ID
	NodeId ID;
	PHub(Schedule schedule, 
		string redezvousUri,
		unordered_map<NodeId, string> nodeToIP,
		vector<float>& sizes,
		vector<void*> applicationSuppliedAddrs,
		int totalParticipant,
		int elementWidth,
		NodeId Id);
	string RendezvousUri;

	vector<void*> ApplicationSuppliedAddrs;
	shared_ptr<Rendezvous> phubRendezvous = NULL;
	shared_ptr<gloo::rendezvous::RedisStore> pRedisStore = NULL;
	shared_ptr<gloo::transport::Device> pGlooDefaultDevice = NULL;
	void InitializeDevice();
	void InitializeDeviceSpecifics();
	void InitializePHubSpecifics();
	void Push(PLinkKey pkey, NodeId target);
	void Pull(PLinkKey pkey, NodeId source);
	void PullOnce(PLinkKey pkey, NodeId source);
	void Broadcast(PLinkKey pkey, vector<NodeId>& targets);
	void Gather(PLinkKey pkey, vector<NodeId>& sources);
	shared_ptr<vector<KeyDesc>> RetrieveKeyDescs()
	{
		return pKeyDescs;
	}
};