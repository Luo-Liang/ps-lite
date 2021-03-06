


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
using namespace gloo::rendezvousString;
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
				//Van::MetadataElision | 
				Van::WorkerSidePushPullZeroCopy |
				Van::SupportsKeyChunking;
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
			if (my_node_.role == Node::SERVER)
			{
				//nothing to do with servers.
				return;
			}

			gloo::transport::ibverbs::attr verbsAttr;
			auto ib = Environment::Get()->find("PHUB_PREFERRED_INTERFACE");
			if (ib == NULL)
			{
				ib = "mlx4_0";
			}
			auto ibstr = std::string(ib);
			verbsAttr.name = ibstr;
			verbsAttr.port = 1;
			verbsAttr.index = 0;
			auto dev = CreateDevice(verbsAttr);

			//initialize weight buffer.
			//WeightBuffers.resize(keySize.size());
			KeySArrays.resize(keySize.size());
			LenSArrays.resize(keySize.size());
			timestamps.resize(keySize.size());
			//GoBuffers.resize(keySize.size());
			LOG(INFO) << my_node().id << " is preparing to redezvous.";
			pContext = std::make_shared<gloo::rendezvousString::Context>(Postoffice::Get()->my_rank(), Postoffice::Get()->num_workers());
			gloo::rendezvousString::RedisStore fileSync("10.1.2.45");
			//printf("[%d] connecting layer %d\n", my_node_.id, i);
			pContext->connectFullMesh(fileSync, dev);
			LOG(INFO) << my_node().id << " is connected to the mesh.";
			int totalBytes = 0;
			for (size_t i = 0; i < keySize.size(); i++)
			{
				totalBytes += keySize.at(i);
				//sleep(1);
				//ss << "[EOF]";
				//printf("[%d], rank %d has connected %d to others.\n", my_node_.id, Postoffice::Get()->my_rank(), i);

				KeySArrays.at(i).push_back(i);
				LenSArrays.at(i).push_back(keySize.at(i) / sizeof(float));
				//WeightBuffers[i].resize(keySize[i] / sizeof(float));
		//GlooBuffers.at(i).resize(keySize.at(i)/sizeof(float));
		//printf("[%d] reducer %d passed address %p with len %d\n", my_node_.id, i, layerAddr.back(), keySize.at(i) / sizeof(float));
		//Postoffice::Get()->Barrier(kWorkerGroup, ss.str());

			}
			int paddedSize = RoundUp(totalBytes / sizeof(float), CACHELINE_SIZE_BYTES);
			GlooBuffers.resize(paddedSize);
			//pFastRecvQ = new PHub::LockFree::PHubLFIntegerQueue(keySize.size(), true);
			auto reducerStr = Environment::Get()->find("PHUB_AGGREGATOR");
			if (reducerStr == NULL)
			{
				reducerStr = "halving_doubling";
			}

			auto reducerString = std::string(reducerStr);
			for (size_t i = 0; i < 1; i++)//keySize.size(); i++)
			{
				usleep(100000);
				std::vector<float*> layerAddr;
				layerAddr.push_back(GlooBuffers.data());
				LOG(INFO) << my_node().id << " initializing reducer " << i << " out of " << keySize.size();
				if (reducerString == "halving_doubling")
				{
					Reducers.push_back(new gloo::AllreduceHalvingDoubling<float>(pContext, layerAddr, GlooBuffers.size()));
				}
				else if (reducerString == "ring_chunked")
				{
					Reducers.push_back(new gloo::AllreduceRingChunked<float>(pContext, layerAddr, keySize.at(i) / sizeof(float)));
				}
			}

			initialized = true;
			auto optimizer = Environment::Get()->find("PHUB_OPTIMIZER");
			if (optimizer == NULL)
			{
				optimizer = "nagtt";
			}
			size_t prefetch_distance = 0x240;
			unordered_map<int, int> consolidated;
			consolidated[0] = paddedSize * sizeof(float);
			OPT = Optimizer::Create(optimizer, Postoffice::Get()->num_workers(), consolidated, prefetch_distance, NULL);
			CHECK_NOTNULL(OPT);

			ps::Postoffice::Get()->Barrier(kWorkerGroup, "GlooCollect");


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
			//std::vector<float*> ptrs(1,test.data());
				//int elementCount = 10;//keySize.at(key) / sizeof(float);
			//printf("ptr[0] == %p. key = %d, sz = %d\n", ptrs[0], key, elementCount);
			if (key == keySize.size() - 1)
			{
				//static gloo::AllreduceHalvingDoubling<float> reducer(pContext, ptrs, elementCount);
				//printf("[%d]Reducing key = %d. %p vs %p\n", my_node_.id, key, vKeyAddress.at(key), msg.data[1].data());
				//for(int i = 0; i < keySize.size(); i++)
				{
					Reducers.at(0)->run();
				}
			}
			//Reducers.at(1).run();
			//printf("[%d]Reduction done = %d\n", my_node_.id, key);
				//vKeyAddress should already have the latest value.
				//Run optimization here.
				//this requires a weight buffer.
			//printf("result = %f\n", test[0]);
				//copy back.
				//memcpy((void*)vKeyAddress.at(key), WeightBuffers.at(key).data(), keySize.at(key));
				//signal recv thread.
				//be nice and use LFQ
			if (SuppressOptimizer == false && OPT != NULL)
			{
				OPT->Update(0, GlooBuffers.data(), GlooBuffers.data(), keySize.at(key) / sizeof(float));
			}

			FastRecvQ.Push(key);
			return 1;//don't care.
		}
		virtual int RecvMsg(Message* msg) override
		{
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
				//every time, poll lfq.
				Key result;
				if (FastRecvQ.TryPop(&result))
				{
					//raise(SIGTRAP);
							//something is locally completed.
							//fake a PUSh ACK.
					msg->meta.body = "";
					msg->meta.control.cmd = Control::EMPTY;
					msg->meta.customer_id = 0;
					msg->meta.head = 0;
					msg->meta.push = 1;
					msg->meta.sender = 8;
					msg->meta.recver = my_node_.id;
					msg->meta.request = 0;
					msg->meta.simple_app = 0;
					msg->meta.timestamp = timestamps.at(result);
					msg->data.clear();
					SArray<float> valArray((float*)vKeyAddress.at(result), keySize.at(result) / sizeof(float));
					msg->AddData(KeySArrays.at(result));
					msg->AddData(valArray);
					msg->AddData(LenSArrays.at(result));
					//printf("[%d] deliverying key = %d, len = %d. %p,%p,%p\n", my_node_.id, result, LenSArrays.at(result)[0], KeySArrays.at(result).data(), valArray.data(), LenSArrays.at(result).data());
					return 1;
				}
				pollCnt++;
			}
		}

		ThreadsafeQueue<Key> FastRecvQ;
		Optimizer* OPT = NULL;
		std::shared_ptr<gloo::rendezvousString::Context> pContext;
		bool SuppressOptimizer = false;
		std::unordered_map<Key, std::vector<float>> WeightBuffers;
		std::vector<gloo::Algorithm*> Reducers;
		std::vector<SArray<Key>> KeySArrays;
		std::vector<SArray<int>> LenSArrays;
		std::vector<int> timestamps;
		std::vector<float> GlooBuffers;
		//use default size.
	};
}  // namespace ps
