#pragma once
#include "zmq_van.h"

#pragma once
/**
*  Copyright (c) 2015 by Contributors
*/
#include <stdlib.h>
#include <thread>
#include <string>
#include "ps/internal/van.h"
#include "dmlc/DIME.h"
#include "PHubAllocator.h"
#include <gloo/context.h>
#include <gloo/allreduce_halving_doubling.h>
#include <gloo/allreduce_ring_chunked.h>
#include <gloo/transport/ibverbs/device.h>
#include <gloo/rendezvous/context.h>
#include <gloo/rendezvous/redis_store.h>
#include <gloo/algorithm.h>
#include "Optimizers.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/threadsafe_queue.h"
//#include "infiniband_van.h"
using namespace gloo;
using namespace gloo::transport;
using namespace gloo::transport::ibverbs;
using namespace gloo::rendezvous;
#if _MSC_VER
#define rand_r(x) rand()
#endif


namespace ps {
	/**
	* \brief ZMQ based implementation
	*/
	class GlooVan : public ZMQVan {
	public:
		GlooVan()
		{
			FeatureSet = Van::PullRequestElision |
				Van::MetadataElision |
				Van::WorkerSidePushPullZeroCopy |
				Van::SupportsKeyChunking |
				Van::SynchronousPush |
				Van::PullRequestElision;
			//also enable optimizer but ignore aggregator.
			auto suppressOpt = Environment::Get()->find("PHUB_SUPPRESS_OPTIMIZER");
			if (suppressOpt != NULL)
			{
				SuppressOptimizer = true;
			}

		}
		virtual string VanType() override
		{
			return "gloo";
		}
		virtual ~GlooVan() { }
		virtual void OnKeyPopulated() override
		{
			if (my_node().role != Node::WORKER) return;
			//create context.
			CHECK(keySize.size() > 0);
			gloo::transport::ibverbs::attr verbsAttr;
			verbsAttr.name = "mlx4_0";
			verbsAttr.port = 1;
			verbsAttr.index = 0;
			auto dev = CreateDevice(verbsAttr);
			if (my_node_.role == Node::SERVER)
			{
				//nothing to do with servers.
				return;
			}

			//initialize weight buffer.
			//WeightBuffers.resize(keySize.size());
			KeySArrays.resize(keySize.size());
			LenSArrays.resize(keySize.size());
			timestamps.resize(keySize.size());
			//GoBuffers.resize(keySize.size());

			pContext = std::make_shared<gloo::rendezvous::Context>(Postoffice::Get()->my_rank(), Postoffice::Get()->num_workers());
			Postoffice::Get()->Barrier(kWorkerGroup, "Context creation finished.");
			//auto redisAddr = Environment::Get()->find("GLOO_REDIS_ADDRESS");
			gloo::rendezvous::RedisStore redisSync("n45");
			pContext->connectFullMesh(redisSync, dev);
			Postoffice::Get()->Barrier(kWorkerGroup, "Waiting for Redis Rendezvous.");
			int totalBytes = 0;
			auto reducerStr = Environment::Get()->find("PHUB_AGGREGATOR");
			if (reducerStr == NULL)
			{
				reducerStr = "halving_doubling";
			}
			auto reducerString = std::string(reducerStr);
			//GlooBuffersLayered.resize(keySize.size());
			GlooBuffersLayeredGradientPtr.resize(keySize.size());			
			for (size_t i = 0; i < keySize.size(); i++)
			{
				totalBytes += keySize.at(i);
				KeySArrays.at(i).push_back(i);
				LenSArrays.at(i).push_back(keySize.at(i) / sizeof(float));
				//only need to aggregate this pointer for virtuak layer i.
				GlooBuffersLayeredGradientPtr.at(i).push_back((float*)vKeyAddress.at(i));
				if (reducerString == "halving_doubling")
				{
					Reducers.push_back(new gloo::AllreduceHalvingDoubling<float>(pContext, GlooBuffersLayeredGradientPtr.at(i), keySize.at(i)));
				}
				else if (reducerString == "ring_chunked")
				{
					Reducers.push_back(new gloo::AllreduceRingChunked<float>(pContext, GlooBuffersLayeredGradientPtr.at(i), keySize.at(i)));
				}
			}
			auto optimizer = Environment::Get()->find("PHUB_OPTIMIZER");
			if (optimizer == NULL)
			{
				optimizer = "nagtt";
			}
			size_t prefetch_distance = 0x240;
			OPT = Optimizer::Create(optimizer, Postoffice::Get()->num_workers(), keySize, prefetch_distance, NULL);
			CHECK_NOTNULL(OPT);
			ps::Postoffice::Get()->Barrier(kWorkerGroup, "Gloo Initialization finished.");
			initialized = true;
			LOG(INFO) << "GLOO Rendezvous done." << " selected aggregator " << reducerString << " selected optimizer " << optimizer;

		}
		bool initialized = false;
		virtual bool FullyInitialized() override
		{
			return initialized;
		}
		virtual int SendMsg(const Message& msg) override
		{
			//printf("[%d] wants to send %s\n", my_node_.id, msg.DebugString().c_str());
				//determine what type of message this is
			if (my_node_.role != ps::Node::Role::WORKER)
			{
				//scheduler and server do not participate in this.
				return ZMQVan::SendMsg(msg);
			}
			//printf("SendMsg should be non-blocking\n");
			//PS_VLOG(2)<< my_node_.id << " sending "<< msg.DebugString();
			//print_stacktrace();
			//return ZMQVan::SendMsg(msg);
			if (msg.meta.control.empty() == false || FullyInitialized() == false || msg.meta.simple_app == true /*We don't know how to deal with simple apps*/)
			{
				//LOG(INFO)<<"data is transmitted through ZMQ "<<msg.DebugString();
				//CHECK_EQ(msg.data.size(), 0);
				//printf("[zmq][%d][%d] ZMQ sending data %s\n",my_node_.id, my_node_.role, msg.DebugString().c_str());
				return ZMQVan::SendMsg(msg);
				//go to ZMQ please.
			}
			//std::lock_guard<std::mutex> lk(mu_);
				//it is a worker push/pull 
				//should be no pull, 
			CHECK(msg.meta.push);
			//now all reduce.
			//retrieve keys.
			CHECK(msg.data.size() > 0);
			//key 0:

			SArray<Key> keys = SArray<Key>(msg.data[0]);
			Key key = keys[0];
			timestamps.at(key) = msg.meta.timestamp;
			Reducers.at(key)->run();
			if (SuppressOptimizer == false && OPT != NULL)
			{
				//TODO: update to yourself.
				//This is wrong in accuracy but correct in performance.
				OPT->Update(0, GlooBuffersLayeredGradientPtr.at(key)[0], GlooBuffersLayeredGradientPtr.at(key)[0], keySize.at(key) / sizeof(float));
			}
			//immediately call RecvMsg, instead of waiting for it to be scheduled.
			Message ack;
			Message* pmsg = &ack;
			pmsg->meta.body = "";
			pmsg->meta.control.cmd = Control::EMPTY;
			pmsg->meta.customer_id = 0;
			pmsg->meta.head = 0;
			pmsg->meta.push = 1;
			pmsg->meta.sender = 8;
			pmsg->meta.recver = my_node_.id;
			pmsg->meta.request = 0;
			pmsg->meta.simple_app = 0;
			pmsg->meta.timestamp = msg.meta.timestamp;
			pmsg->data.clear();
			//SArray<float> valArray((float*)vKeyAddress.at(result), keySize.at(result) / sizeof(float));
			//pmsg->AddData(KeySArrays.at(result));
			//pmsg->AddData(valArray);
			//pmsg->AddData(LenSArrays.at(result));
			//FastRecvQ.Push(key);
			RecvMsg(pmsg);
			return 1;//don't care.
		}
		virtual int RecvMsg(Message* msg) override
		{
			return ZMQVan::RecvMsg(msg);
			//I really do not care about any messages. 
			//GlooVans are metadata elision and pull elision enabled.
			if (my_node_.role != Node::WORKER)
			{
				return ZMQVan::RecvMsg(msg);
			}

			//real worker. I only receive "locally completed optimization" and control messages.
			static uint64_t pollCnt = 0;
			const uint64_t pollZMQEvery = 16383;
			while (true)
			{
				if ((pollCnt & pollZMQEvery) == 0 || FullyInitialized() == false)
				{
					if (ZMQVan::RecvCtrlMsgNoWait(msg) > 0)
					{
						if (msg->meta.control.empty() && msg->data.size() == 3)
						{
							//this is initialization
							auto key = SArray<Key>(msg->data[0])[0];
							if (key >= 0)
							{
								auto val = SArray<float>(msg->data[1]);
								auto len = SArray<int>(msg->data[2])[0];
								CHECK(msg->data.size() == 3);
								//CHECK(msg->data[2].size() == 1);
								CHECK(FullyInitialized() == false) << " Infiniband must not be fully initialized.";
								CHECK(keySize[key] == len * sizeof(float));
								auto& vec = WeightBuffers[key];
								vec.resize(len);
								memcpy(vec.data(), val.data(), len * sizeof(float));
							}
						}
						return 1;
					}
				}
				////every time, poll lfq.
				//Key result;
				//if (FastRecvQ.WaitAndPopTryPop(&result))
				//{
				//	//raise(SIGTRAP);
				//			//something is locally completed.
				//			//fake a PUSh ACK.
				//	msg->meta.body = "";
				//	msg->meta.control.cmd = Control::EMPTY;
				//	msg->meta.customer_id = 0;
				//	msg->meta.head = 0;
				//	msg->meta.push = 1;
				//	msg->meta.sender = 8;
				//	msg->meta.recver = my_node_.id;
				//	msg->meta.request = 0;
				//	msg->meta.simple_app = 0;
				//	msg->meta.timestamp = timestamps.at(result);
				//	msg->data.clear();
				//	SArray<float> valArray((float*)vKeyAddress.at(result), keySize.at(result) / sizeof(float));
				//	msg->AddData(KeySArrays.at(result));
				//	msg->AddData(valArray);
				//	msg->AddData(LenSArrays.at(result));
				//	//printf("[%d] deliverying key = %d, len = %d. %p,%p,%p\n", my_node_.id, result, LenSArrays.at(result)[0], KeySArrays.at(result).data(), valArray.data(), LenSArrays.at(result).data());
				//	return 1;
				//}
				//pollCnt++;
			}
		}

		ThreadsafeQueue<Key> FastRecvQ;
		Optimizer* OPT = NULL;
		std::shared_ptr<gloo::rendezvous::Context> pContext;
		bool SuppressOptimizer = false;
		std::unordered_map<Key, std::vector<float>> WeightBuffers;
		std::vector<gloo::Algorithm*> Reducers;
		std::vector<SArray<Key>> KeySArrays;
		std::vector<SArray<int>> LenSArrays;
		std::vector<int> timestamps;
		std::vector<std::vector<float*>> GlooBuffersLayeredGradientPtr;
		//std::vector<std::vector<float>> GlooBuffersLayeredWeightPtr;
		std::vector<float*> layerAddrs;
		//use default size.
	};
}  // namespace ps
