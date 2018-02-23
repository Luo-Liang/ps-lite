#pragma once
#include <vector>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <unordered_map>
#include "Schedule.h"
#include "../dmlc/logging.h"
#include <gloo/context.h>
#include <gloo/allreduce_halving_doubling.h>
#include <gloo/allreduce_ring_chunked.h>
#include <gloo/transport/ibverbs/device.h>
#include <gloo/rendezvous/context.h>
#include <gloo/rendezvous/redis_store.h>
#include <gloo/algorithm.h>
#include "internal/ext.h"
#include "rendezvous.h"
#include "internal/PHubAllocator.h"
#include "internal/PHubStructures.h"
#include "phub.h"
using namespace std;
typedef uint32_t NodeId;
typedef uint64_t BufferHandle;
typedef uint32_t PLinkKey;

#define ToBufferHandle(nid, handle) ((BufferHandle)(nid << 32 | handle))
#define NodeIdFromHandle(handle) ((NodeId)(handle >> 32))
#define KeyFromHandle(handle) ((PLinkKey)(handle & 0xFFFFFFFF))
struct PLinkContinuation
{
	//continuation points to a closure.
	//contains a function. this function is simply a schedule step id in PLink.
	PLinkKey Key;
	//TODO: Operator must be non-blocking.
	//Gloo is now using blocking ops.
	shared_ptr<IOperator> Op;
};

struct PLinkWorkQueue
{
	//only one pending operation per key or layer is needed
	//a thread simply scans the keys it is in charge of.
	int KeyCount;
	PLinkWorkQueue(int keyCount)
	{
		KeyCount = keyCount;
		WorkQueue = new PLinkContinuation[keyCount];
		memset(WorkQueue, NULL, sizeof(WorkQueue) * KeyCount);
	}
	PLinkContinuation* WorkQueue;
};

class PLinkExecutor
{
	//a phub
	//a set of gloo algorithms
	//a workqueue
	shared_ptr<PLinkWorkQueue> wQs;
	shared_ptr<PHub> pHub;
public:
	void Initialize(unordered_map<PLinkKey, Schedule> schedules,
		string redezvousUri,
		unordered_map<NodeId, string> nodeToIP,
		vector<float>& sizes,
		vector<void*> applicationSuppliedAddrs,
		int totalParticipant,
		int elementWidth,
		NodeId Id);
};