#pragma once
#include "dmlc/logging.h"
#include "dmlc/DIME.h"
#include <ps/internal/van.h>
#include <ps/internal/postoffice.h>
#include <ps/internal/message.h>
#include <sstream>
#include <malloc.h> //memalign
#include "Optimizers.h"
#include <signal.h>
#include <limits.h>
#include "Verbs.hpp"
/*#define PHUB_MAX_KEY_BITS 22
#define PHUB_MAX_KEY ((1<<PHUB_MAX_KEY_BITS) - 1)
#define PHUB_IMM_KEY_MASK PHUB_MAX_KEY
#define PHUB_IMM_SENDER_MASK ((1<<(32 - PHUB_MAX_KEY_BITS))-1)*/

namespace ps
{
	//PSHUB requires only 1 sge per message, instead of 2.
	//PSHUB requires sending out 2 messages.
	//////////////////////////////////////////////////////////////////////////
	//For PHUB use only.
	struct PHubMergeBuffer
	{
	public:
		//volatile RemainingReadersForCurrentBuffer; //how many people in the current iteration have pulled?
		volatile int WriteBufferIndex __attribute__((aligned(CACHELINE_SIZE_BYTES))); //either 0 or 1. Buffer used for current iteration.
		//the actual buffer size that's used to align data for sse.
		size_t ActualBufferSizePaddedForSSE;
		size_t ActualElementCountPaddedForSSE;
		//ibv_send_wr ReceiveRequestFastAckSendRequest;
		int Length;
		int FilledLength;
		Key KeyVal;
		Verbs* AssociatedVerbs;
		bool initialized;
		int CQIndex = -1;
		bool PSInitialized;//the first push by rank 0 worker
		int QPOffset;

		PHubMergeBuffer()
		{
			initialized = false;
			PSInitialized = false;
		}

		bool Initialized()
		{
			return initialized;
		}
		std::vector<ibv_send_wr> RDMAWorkerSendKVRequests;
		std::vector<int> remoteQPIdxs;

		std::vector<std::vector<ibv_sge>> RDMAWorkerSendSgesArray;
		void Init(Verbs* verbs, int bufferLen, Key key,
			char* pBuf1,
			char* pBuf2,
			std::vector<uint64_t> rdmaRemoteKVBuffers)
		{
			CHECK(pBuf1 != NULL);
			CHECK(pBuf2 != NULL);
			CHECK(verbs != NULL);
			//CHECK(sizeof(MetaSlim) % (INSTRUCTION_VECTOR_SIZE * sizeof(float)) == 0) << "MetaSlim's size made it not possible to align subsequent floats in MergeBuffer";
			Length = bufferLen;
			AssociatedVerbs = verbs;
			//RemainingReadersForCurrentBuffer = ps::Postoffice::Get()->num_workers();
			WriteBufferIndex = 0;
			//auto kvLen = bufferLen - sizeof(MetaSlim); // this is a terrible impl;
			ActualElementCountPaddedForSSE = RoundUp(bufferLen / sizeof(float), INSTRUCTION_VECTOR_SIZE);
			ActualBufferSizePaddedForSSE = ActualElementCountPaddedForSSE * sizeof(float);
			Buffer1 = pBuf1;
			memset(Buffer1, 0, ActualBufferSizePaddedForSSE);
			Buffer2 = pBuf2;
			memset(Buffer2, 0, ActualBufferSizePaddedForSSE);

			//The first sizeof(MetaSlim) bytes in buf1 and buf2 are not used in RDMA
			//copy it.
			CHECK(ps::Postoffice::Get()->num_workers() == numWorkers);
			ibv_send_wr cleanWr;
			ibv_sge cleanSge;
			memset(&cleanSge, 0, sizeof(ibv_sge));
			memset(&cleanWr, 0, sizeof(ibv_send_wr));
			RDMAWorkerSendKVRequests.resize(numWorkers, cleanWr);
			remoteQPIdxs.resize(numWorkers, -1);
			RDMAWorkerSendSgesArray.resize(numWorkers);

			KeyVal = key;

			auto qpIdx = verbs->workerKey2QPIdx.at(KeyVal);
			auto srvIdx = verbs->workerQP2SrvId.at(qpIdx);
			CHECK(srvIdx == ps::Postoffice::Get()->my_rank());
			QPOffset = verbs->workerQP2SrvQPIdx.at(qpIdx);
			for (size_t i = 0; i < numWorkers; i++)
			{
				RDMAWorkerSendSgesArray[i].resize(2, cleanSge);

				//SendKVRequests
				RDMAWorkerSendKVRequests.at(i).num_sge = 1;
				RDMAWorkerSendKVRequests.at(i).sg_list = &RDMAWorkerSendSgesArray[i][1];
				RDMAWorkerSendKVRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
				RDMAWorkerSendKVRequests.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
				RDMAWorkerSendKVRequests.at(i).wr.rdma.remote_addr = rdmaRemoteKVBuffers[i];
				RDMAWorkerSendKVRequests.at(i).wr.rdma.rkey = verbs->Helper_Server_GetEndpointFromKey(key, i).RemoteKey;


				//SendMetaRequests
				RDMAWorkerSendMetaRequests.at(i).num_sge = 1;
				RDMAWorkerSendMetaRequests.at(i).sg_list = &RDMAWorkerSendSgesArray[i][0];
				RDMAWorkerSendMetaRequests.at(i).opcode = IBV_WR_RDMA_WRITE;
				RDMAWorkerSendMetaRequests.at(i).next = &RDMAWorkerSendKVRequests[i];
				RDMAWorkerSendMetaRequests.at(i).wr.rdma.remote_addr = (uint64_t)rdmaRemoteMetaBuffers[i];
				RDMAWorkerSendMetaRequests.at(i).wr.rdma.rkey = verbs->Helper_Server_GetEndpointFromKey(key, i).RemoteKey;


				//SendKVSges.
				RDMAWorkerSendSgesArray[i][1].length = Length - sizeof(MetaSlim);
				//enable bit reduction?
				if (RDMAWorkerSendSgesArray[i][1].length / verbs->VerbsBitwidthReductionRatio > sizeof(float))
				{
					RDMAWorkerSendSgesArray[i][1].length /= verbs->VerbsBitwidthReductionRatio;
				}
				CHECK(RDMAWorkerSendSgesArray[i][1].addr == NULL); //<--variable.

				//SendMetaSges.
				RDMAWorkerSendSgesArray[i][0].length = sizeof(MetaSlim);
				RDMAWorkerSendSgesArray[i][0].addr = rdmaWorkerRecvBufferMetaAddrs[i];


				remoteQPIdxs.at(i) = QPOffset + verbs->psQPCount * i;

				if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision))
				{
					//with metadata elision, we use the RDMAWorkerSendMetaRequests to actually transport payload
					//terrible but allows us to change less code.
					RDMAWorkerSendMetaRequests.at(i).num_sge = 1;
					RDMAWorkerSendMetaRequests.at(i).sg_list = &RDMAWorkerSendSgesArray.at(i)[1];
					RDMAWorkerSendMetaRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
					//index 1 is KV.
					RDMAWorkerSendMetaRequests.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
					RDMAWorkerSendMetaRequests.at(i).next = NULL;
					//override the remote address.
					RDMAWorkerSendMetaRequests.at(i).wr.rdma.remote_addr = (uint64_t)rdmaRemoteKVBuffers[i];

				}
				//we can save 1 message if the meta and kv are continuous on the worker side. test it here.
				else if (rdmaRemoteMetaBuffers.at(i) + sizeof(MetaSlim) == rdmaRemoteKVBuffers.at(i))
				{
					CHECK(false) << " Elision off?";
					//do not use two requests.
					//make sure the correct opcode is used.
					RDMAWorkerSendMetaRequests.at(i).num_sge = 2; //one for meta and one for kv.
					RDMAWorkerSendMetaRequests.at(i).sg_list = RDMAWorkerSendSgesArray.at(i).data();
					RDMAWorkerSendMetaRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
					RDMAWorkerSendMetaRequests.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
					RDMAWorkerSendMetaRequests.at(i).next = NULL;
				}
				else
				{
					CHECK(false) << " Elision off?";
				}
			}

			CHECK(((uint64_t)Buffer1 & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0) << " requested alignment is " << INSTRUCTION_VECTOR_SIZE * sizeof(float);
			CHECK(((uint64_t)Buffer2 & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
			//printf("mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, key);
			ActualElementCountPaddedForSSE = (ActualBufferSizePaddedForSSE - sizeof(MetaSlim)) / sizeof(float);
			CHECK(KeyVal < PHUB_MAX_KEY);
			initialized = true;

		}

		inline int GetKVBufferLen()
		{
			return Length - sizeof(MetaSlim);
		}

		inline int GetKVBufferLenPaddedForSSE()
		{
			return ActualBufferSizePaddedForSSE - sizeof(MetaSlim);
		}

		inline int GetKVElementCountAccountingForSSEPadding()
		{
			return ActualElementCountPaddedForSSE;
		}

		inline char* GetCurrentWriteBuffer()
		{
			//write buffer is always opposite from pull buffer.
			auto ret = WriteBufferIndex == 0 ? Buffer1 : Buffer2;
			//printf("[W]mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, KeyVal);

			return ret + sizeof(MetaSlim);
		}

		inline char* GetCurrentReadBuffer()
		{
			CHECK(PSInitialized);
			auto ret = WriteBufferIndex == 0 ? Buffer2 : Buffer1;
			//printf("[r]mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, KeyVal);

			return ret + sizeof(MetaSlim);
		}

		void AggregationAndOptimizationReady()
		{
			WriteBufferIndex = (WriteBufferIndex + 1) & 1;
		}

		///Cookies injected.
		///For non-accurate optimizations, make sure to check the first Sizeof(metaSlim) buffer is not touched.
		void PostSendBufferBundledPushPullAck(int remoteMachine)
		{
			auto buffer = GetCurrentReadBuffer();
			//o is meta, 1 is kv. 
			RDMAWorkerSendSgesArray[remoteMachine][1].addr = (uint64_t)buffer;
			auto& ep = AssociatedVerbs->Helper_Server_GetEndpointFromKey(KeyVal, remoteMachine);
			CQIndex = ep.CQIdx;
			auto lkey = AssociatedVerbs->DeviceMemoryRegions.at(ep.DeviceIdx)->lkey;
			//use the correct lkeys.
			//RDMAWorkerSendMetaSges.at(remoteMachine).lkey = RDMAWorkerSendKVSges[remoteMachine].lkey = lkey;
			//just in case.
			RDMAWorkerSendSgesArray.at(remoteMachine).at(0).lkey = RDMAWorkerSendSgesArray.at(remoteMachine).at(1).lkey = lkey;
			CHECK(remoteQPIdxs.at(remoteMachine) == ep.Index);
			//auto pMs = (MetaSlim*)RDMAWorkerRecvBufferMetaAddrs.at(remoteMachine);
		//printf("[PSHUB] Sending Bundled message to QPidx = %d, rid = %d \n", remoteQPIdxs.at(remoteMachine), 9 + remoteMachine * 2);
			int msgCnt = RDMAWorkerSendMetaRequests[remoteMachine].next == NULL ? 1 : 2;
			//metadata elision
			CHECK(msgCnt == 1);
			//printf("bundle message count = %d\n", msgCnt);
			AssociatedVerbs->VerbsSmartPost(remoteQPIdxs.at(remoteMachine), CQIndex, msgCnt, &(RDMAWorkerSendMetaRequests.at(remoteMachine)));
		}
		char* Buffer1;
		char* Buffer2;
	};

	struct IBReceiveRequest
	{
		size_t QPIndex __attribute__((aligned(CACHELINE_SIZE_BYTES)));
		Verbs* AssociatedVerbs;
		ibv_recv_wr ReceiveRequest;
		IBReceiveRequest(size_t qpIndex, Verbs* verbs, size_t id)
		{
			QPIndex = qpIndex;
			AssociatedVerbs = verbs;
			memset(&ReceiveRequest, 0, sizeof(ibv_recv_wr));
			ReceiveRequest.wr_id = id;
		}

		void PostReceiveRequest()
		{
			//printf("[%d]Post Recv Request QPI=%d, ID=%d\n", ps::Postoffice::Get()->van()->my_node().id,  QPIndex,  ReceiveRequest.wr_id);
			AssociatedVerbs->post_receive(QPIndex, &ReceiveRequest);
		}
	};
}
