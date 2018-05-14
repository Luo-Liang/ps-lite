#pragma once
#include "infiniband_van.h"
#include "dmlc/logging.h"
#include <thread>
#include <vector>
#include "Verbs.hpp"
#include <atomic>
#include <sstream>
#include <math.h>
//#include "lfq.h"
#include "dmlc/DIME.h"
#include "PHUB.h"
#include "Optimizers.h"
#include "Helpers.h"
using namespace std;
namespace ps
{
//struct atomicwrapper
//{
//    std::atomic_int core{ 0 };
//    atomicwrapper &operator=(const atomicwrapper &other)
//    {
//        core.store(other.core.load());
//    }
//    atomicwrapper() :core() {}
//    atomicwrapper(const std::atomic_int &a) :core(a.load()) {}
//    atomicwrapper(const atomicwrapper &other) :core(other.core.load()) {}
//};
class PSHUBVan : public InfiniBandVan
{
  public:
    int MaximumAllowedThreads;
    //one for each key.
    vector<vector<PHubMergeBuffer>> PerSocketMergeBuffers;
    bool SuppressAggregator = false;
    //vector<bool> isKeyInit;
    //looks like timestamp is th eonly thing that changes.
    //sender to timestamp mapping.
    vector<atomic_int *> UpdatesReceivedBuffer;
    //how many copies of data is merged for the same key?
    std::atomic<int> ai{0};
    bool IsAsync = false;
    bool SuppressOptimizer = false;
    Optimizer *OPT = NULL;
    Aggregator *ADD = NULL;
    vector<int> selfLoopKeyCounter;
    //let infiniband van deal with all those start up thingy.
    //leave send as it is.
    //intercept push calls
    int selfLoopExpectedCounter = 2;
    PSHUBVan(int maxDataServingThreads)
    {
        auto async = Environment::Get()->find("PHUB_ASYNC_MODE");
        if (async != NULL)
        {
            IsAsync = true;
        }
        auto suppressOpt = Environment::Get()->find("PHUB_SUPPRESS_OPTIMIZER");
        if (suppressOpt != NULL)
        {
            SuppressOptimizer = true;
        }
        auto suppressAgg = Environment::Get()->find("PHUB_SUPPRESS_AGGREGATOR");
        if (suppressAgg != NULL)
        {
            SuppressAggregator = true;
        }
        auto selfLoopExpectedCounterStr = Environment::Get()->find("PHUB_HIERARCHICAL_SIM_RACK");
        if (selfLoopExpectedCounterStr != NULL)
        {
            selfLoopExpectedCounter = atoi(selfLoopExpectedCounterStr);
        }
        //our job is to distribute these threads to different jobs.
        this->MaximumAllowedThreads = maxDataServingThreads;
        FeatureSet = Van::WorkerSidePushPullZeroCopy |
                     Van::OnlyDeliversMergedBuffer |
                     Van::SupportsKeyChunking |
                     Van::NativeInfiniband |
                     Van::VerbsPostSendNoLocking |
                     Van::SupportsBitwidthSIM |
                     Van::PullRequestElision |
                     Van::MetadataElision;
    }
    ~PSHUBVan()
    {
        //what is this??
        int tst_val = 0;
        int new_val = 1;
        auto exchanged = ai.compare_exchange_strong(tst_val, new_val);
        if (exchanged == false)
            return;
    }
    std::vector<std::thread> threads;
    //things should start after OnKeyPopulated.
    //For SendMsg/RecvMsg we dont care.
    //Override only RecvMsg.
    std::string ShowImbalance()
    {
        std::stringstream ss;
        ss << "Proc Workloads:";
        std::vector<size_t> loads;
        loads.resize(GetUnderlyingWorkerThreadCount());

        std::vector<size_t> IFLoads;
        IFLoads.resize(verbs->virtual_devices.size());
        std::vector<size_t> CQKeys;
        CQKeys.resize(verbs->sendCompletionQueues.size());
        for (size_t i = 0; i < machineCount; i++)
        {
            for (size_t j = 0; j < keySize.size(); j++)
            {
                auto qp = verbs->workerKey2QPIdx.at(j);
                if (verbs->workerQP2SrvId.at(qp) == ps::Postoffice::Get()->my_rank())
                {
                    auto &ep = verbs->Helper_Server_GetEndpointFromKey(j, i);
                    auto coreIdx = ep.CoreIdx;
                    loads[coreIdx] += keySize[j];
                    auto ifIdx = ep.DeviceIdx;
                    IFLoads.at(ifIdx) += keySize[j];
                    auto idx = ep.CQIdx;
                    CQKeys.at(idx)++;
                }
            }
        }
        for (int i = 0; i < GetUnderlyingWorkerThreadCount(); i++)
        {
            ss << " [Proc " << i << "]:" << loads[i] / 1024.0 / 1024.0 << " MB";
            //which key is mapped to a CQ?
        }

        ss << " Interface Workloads:";
        for (size_t i = 0; i < IFLoads.size(); i++)
        {
            ss << " [" << verbs->device_names.at(i) << ":" << verbs->ports.at(i) << "]" << IFLoads[i] / 1024.0 / 1024.0 << " MB";
        }
        ss << " Max CQ Load = " << *std::max_element(CQKeys.begin(), CQKeys.end());
        return ss.str();
    }

    void virtual InitializeVanOptimizerValues(uint num, float *values) override
    {
        if (my_node().role == Node::SERVER)
        {
            CHECK(OPT != NULL || SuppressOptimizer);
            if (OPT != NULL)
            {
                OPT->PopulateTrainingParams(num, values);
            }
        }
    }

    void OnKeyPopulated() override
    {
        InfiniBandVan::OnKeyPopulated();
        if (my_node().role != Node::SERVER)
            return;
        //CHECK(verbs->CoreCount > 0);
        selfLoopKeyCounter.resize(keySize.size());
        auto prefetch_dist_str = Environment::Get()->find("PHUB_PREFETCH_DIST");
        size_t prefetch_distance = 0x240;
        if (prefetch_dist_str != NULL)
        {
            prefetch_distance = atoi(prefetch_dist_str);
        }

        auto optimizer = Environment::Get()->find("PHUB_OPTIMIZER");
        if (optimizer == NULL)
        {
            optimizer = "nagntnt";
        }
        OPT = Optimizer::Create(optimizer, machineCount, keySize, prefetch_distance, verbs);
        CHECK_NOTNULL(OPT);

        auto aggregator = Environment::Get()->find("PHUB_AGGREGATOR");
        if (aggregator == NULL)
        {
            aggregator = "ntnt";
        }
        ADD = Aggregator::Create(aggregator, prefetch_distance);
        CHECK_NOTNULL(ADD);

        /*if (MaximumAllowedThreads > keySize.size())
            {
                MaximumAllowedThreads = keySize.size();
        }*/
        //UpdatesReceivedBuffer.resize(keySize.size());
        //per socket aggregation.
        //use migration to perform resize.
        for (size_t sockId = 0; sockId < verbs->SocketCount; sockId++)
        {
            MigrateToNumaNode(sockId);
            PerSocketMergeBuffers.push_back(vector<PHubMergeBuffer>(keySize.size()));
            //make sure the last one's address is on the right numa node.
            if (PerSocketMergeBuffers.size() > 0 && GetAddressNumaNode(&PerSocketMergeBuffers.back()) != sockId)
            {
                LOG(INFO) << " Per socket merge buffer allocation per sock = " << sockId << " failed to allocate on desired socket.";
            }
        }
        //PerSocketMergeBuffers.resize(verbs->SocketCount, vector<PHubMergeBuffer>(keySize.size()));

        //isKeyInit.resize(keySize.size(),true);
        //resize each vector in merge buffer.
        int maxKeySizeinB = 0;
        Key kid = -1;
        size_t totalSizeInMB = 0;
        auto allocator = PHubAllocator::Get();
        CHECK(allocator->IsInitialized());
        size_t len = 0;
        UpdatesReceivedBuffer.resize(keySize.size(), NULL);
        for (size_t i = 0; i < keySize.size(); i++)
        {
            //if its DirectConnect, UpdatesReceivedBuffer is allocated in one of the NUMA node.
            if (verbs->DirectConnect)
            {
                UpdatesReceivedBuffer.at(i) = new atomic_int(0);
                CHECK(*(UpdatesReceivedBuffer.back()) == 0);
            }
            //is this key for me?
            auto wQP = verbs->workerKey2QPIdx[i];
            auto target = verbs->workerQP2SrvId[wQP];
            if (target != ps::Postoffice::Get()->IDtoRank(my_node_.id))
            {
                //printf("[%d] key %d is assigned to %d, not me\n", ps::Postoffice::Get()->my_rank(), i, target);
                continue;
                //my idx is calculated this way.
            }
            CHECK(keySize.find(i) != keySize.end());
            //for RDMA only:
            //send in recvBuffer's meta address.
            //send in remote's kv buffer.
            //send in remote's meta buffer.
            std::vector<uint64_t> rdmaRecvMetaBuffersPerWorker(machineCount, 0);
            std::vector<uint64_t> rdmaRemoteWorkerKVBufferAddr(machineCount, 0);
            std::vector<uint64_t> rdmaRemoteWorkerMetaBufferAddr(machineCount, 0);
            //populate those 3 vectors.
            CHECK(machineCount == ps::Postoffice::Get()->num_workers());

            //when direct connect is disabled, the per socket merge buffer collapse into one single buffer.
            for (int sid = 0; sid < PerSocketMergeBuffers.size(); sid++)
            {
                for (size_t mid = 0; mid < machineCount; mid++)
                {
                    size_t len;
                    rdmaRecvMetaBuffersPerWorker[mid] = (uint64_t)allocator->PHUBReceiveMetaBuffer(i, mid, sid, len);
                    rdmaRemoteWorkerKVBufferAddr[mid] = (uint64_t)remoteKVAddrs.at(mid)[i];
                    CHECK(rdmaRemoteWorkerKVBufferAddr[mid] != 0);
                    rdmaRemoteWorkerMetaBufferAddr[mid] = (uint64_t)remoteMetaAddrs.at(mid)[i];
                    CHECK(rdmaRemoteWorkerMetaBufferAddr[mid] != 0);
                }
                //auto& ep = verbs->Helper_Server_GetEndpointFromKey(i, 0);
                //CHECK(sid == ep.SocketIdx);
                PerSocketMergeBuffers.at(sid).at(i).Init(verbs,
                                                         keySize[i] + sizeof(MetaSlim),
                                                         i,
                                                         //note that this assignment is only valid for switch-enabled.
                                                         //direct connect requires overwriting LKey when pushing out because it's different interface.
                                                         //there is only 1 copy
                                                         (char *)allocator->PHUBMergeMetaBuffer(i, 0, sid, len),
                                                         //not used
                                                         (char *)allocator->PHUBMergeMetaBuffer1(i, 0, sid, len),
                                                         rdmaRecvMetaBuffersPerWorker,
                                                         rdmaRemoteWorkerKVBufferAddr,
                                                         rdmaRemoteWorkerMetaBufferAddr);
                PerSocketMergeBuffers.at(sid).at(i).PSInitialized = true;
                allocator->VerifyPHUBMergeKV1(sid, i, PerSocketMergeBuffers[sid][i].GetCurrentReadBuffer(), PerSocketMergeBuffers[sid][i].GetCurrentReadBuffer() + PerSocketMergeBuffers[sid][i].GetKVBufferLenPaddedForSSE());
                allocator->VerifyPHUBMergeKV(sid, i, PerSocketMergeBuffers[sid][i].GetCurrentWriteBuffer(), PerSocketMergeBuffers[sid][i].GetCurrentWriteBuffer() + PerSocketMergeBuffers[sid][i].GetKVBufferLenPaddedForSSE());
                allocator->VerifyPHUBMergeMeta(sid, i, PerSocketMergeBuffers[sid][i].Buffer1, PerSocketMergeBuffers[sid][i].Buffer1 + sizeof(MetaSlim));
                allocator->VerifyPHUBMergeMeta1(sid, i, PerSocketMergeBuffers[sid][i].Buffer2, PerSocketMergeBuffers[sid][i].Buffer2 + sizeof(MetaSlim));
                CHECK(PerSocketMergeBuffers[sid][i].GetCurrentReadBuffer() == PerSocketMergeBuffers[sid][i].Buffer2 + sizeof(MetaSlim));
                CHECK(PerSocketMergeBuffers[sid][i].GetCurrentWriteBuffer() == PerSocketMergeBuffers[sid][i].Buffer1 + sizeof(MetaSlim));
                if (initializer.find(i) != initializer.end())
                {
                    //some tests may not have this.
                    CHECK(initializer[i].size() * sizeof(float) == keySize[i]);
                    memcpy(PerSocketMergeBuffers.at(sid).at(i).GetCurrentReadBuffer(), initializer[i].data(), keySize[i]);
                }
                else
                {
                    //CHECK(false);
                    memset(PerSocketMergeBuffers.at(sid).at(i).GetCurrentReadBuffer(), 0, keySize[i]);
                }
            }

            if (keySize[i] > maxKeySizeinB)
            {
                maxKeySizeinB = keySize[i];
                kid = i;
            }

            totalSizeInMB += keySize[i];
        }

        auto hc = std::thread::hardware_concurrency();
        //how many cores in real sockets.
        auto perSocket = hc / verbs->SocketCount;
        //how many in virtual sockets
        auto perSocketAdjusted = MaximumAllowedThreads / verbs->SocketCount;
        CHECK(perSocket * verbs->SocketCount == hc) << "Do you have an unbalanced socket? ";
        CHECK(perSocketAdjusted * verbs->SocketCount == MaximumAllowedThreads) << "Cannot evenly divide requested number of processors to each sockets";
        CHECK(MaximumAllowedThreads <= hc) << "You are requesting more threads than there are cores.";
        for (size_t i = 0; i < MaximumAllowedThreads; i++)
        {
            int socketId = i / perSocketAdjusted;
            int socketOffset = i % perSocketAdjusted;
            int cpuid = socketId * perSocket + socketOffset;
            threads.push_back(thread(&PSHUBVan::infiniBandCollect, this, i));
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(cpuid, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0)
            {
                printf("[PSHUB][%d] Thread affinity for %d has failed\n", my_node_.id, i);
                CHECK(false);
            }
            else
            {
                //printf("[PSHUB] mapping thread %d to cpuid %d, in socket = %d\n", i, cpuid, socketId);
            }
        }
        GTG = true;
        PS_VLOG(0) << "[PSHUB" << my_node_.id << "] Sim Racks = " << selfLoopExpectedCounter  << " Maximum Key=" << kid << " Size=" << maxKeySizeinB / 1024.0 << " kB. There are " << keySize.size() << " keys. Total: " << totalSizeInMB / 1024.0 / 1024.0 << " MB. Running Mode: Async = " << IsAsync << " Suppress Optimizer = " << SuppressOptimizer << " Optimizer = " << optimizer << " Aggregator = " << aggregator << " Suppress Aggregator =" << SuppressAggregator << " " << ShowImbalance();
    }
    volatile bool stopRequested = false;
    std::string DebugString(std::vector<pair<int, int>> &data)
    {
        std::stringstream ss;
        ss << "disect:\n";
        for (auto item : data)
        {
            ss << "(" << item.first << "," << item.second << ")";
        }
        ss << "\n";
        return ss.str();
    }
    std::unordered_map<Key, std::vector<float>> initializer;

    virtual int RecvMsg(Message *msg) override
    {
        if (my_node_.role == Node::WORKER)
        {
            return InfiniBandVan::RecvMsg(msg);
        }
        else if (my_node_.role == Node::SCHEDULER)
        {
            return ZMQVan::RecvMsg(msg);
        }
        else
        {
            //intercept init data
            //completely detach PHUB with PS-LITE pipeline
            //we need to intercept initialization data packages.
            while (true)
            {
                auto ret = ZMQVan::RecvMsg(msg);
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
                        auto &vec = initializer[key];
                        vec.resize(len);
                        memcpy(vec.data(), val.data(), len * sizeof(float));
                        //need to quickly acknowledge the sender
                        /*Message ack;
                            ack.data.clear();
                            ack.meta.sender = my_node().id;
                            ack.meta.recver = msg->meta.sender;
                            ack.meta.request = 0;
                            ack.meta.simple_app = 0;
                            ack.meta.customer_id = 0;
                            ack.meta.timestamp = msg->meta.timestamp;
                            ack.meta.head = 0;*/
                        //done.
                        return ret;
                    }
                    else
                    {
                        //an init.
                        //I could have intercepted it here. but...
                        CHECK(msg->meta.control.cmd == Control::INFINIBANDKEYSIZEEXCHG);
                    }
                }
                /* else if (msg->meta.simple_app == 1 && msg->meta.control.empty() && msg->meta.body.size() > 0) */
                /* { */
                /*   //void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) { */

                /* } */
                else
                {
                    return ret;
                }
            }
        }
    }
    virtual void Stop() override
    {
        //printf("Shutting down...\n");
        if (my_node_.role == Node::SERVER)
        {
            //printf("PSHUB Shutting down...\n");
            stopRequested = true;
            for_each(threads.begin(), threads.end(), [](std::thread &x) { x.join(); });
        }
        InfiniBandVan::Stop();
        //printf("Infiniband Shutting Down...\n");
    }

    virtual std::string VanType() override
    {
        return "pshub";
    }

    //must unify all these Smmarize calls
    std::string Summarize(std::vector<float> &v, int firstN = 10)
    {
        auto ptr = (float *)v.data();
        auto sz = v.size();
        auto sigptr = (int *)ptr;
        int result = 0;
        std::stringstream ss;
        ss << "Vector Summary:(";
        for (size_t i = 0; i < firstN && i < sz; i++)
        {
            ss << ptr[i] << ",";
        }
        ss << ")";
        for (size_t i = 0; i < sz; i++)
        {
            result ^= sigptr[i];
        }
        ss << " Signature:" << result;
        return ss.str();
    }

    void PerCoreNUMAAwareInitialization(size_t tid)
    {
        //first, everyone need to setup their QP Counters.
        //figure out what QPs belong to me.
        std::vector<int> myQPs;
        for (int qpIdx = 0; qpIdx < verbs->endpoints.size(); qpIdx++)
        {
            if (verbs->endpoints.at(qpIdx).CoreIdx == tid)
            {
                myQPs.push_back(qpIdx);
            }
        }
        if (myQPs.size() == 0)
            return;
        //create a buffer large enough to hold all counters.
        //this buffer must from our socket.
        auto mySock = verbs->Core2SocketIdx.at(tid);
        CHECK(mySock == verbs->endpoints.at(myQPs.back()).SocketIdx);
        //create a large, Cache-Aligned buffer on my socket for my QPStat counters.
        //There're only a few QPs. Do not use 2MB alignment
        int *arr = (int *)AlignedAllocateUniversal(RoundUp(myQPs.size() * sizeof(int), CACHELINE_SIZE_BYTES), mySock, CACHELINE_SIZE_BYTES);
        CHECK(arr != NULL) << "Cannot allocate queue pair counter on numa node " << mySock << ". Reduce queue pairs?";
        //put it in QPStats.
        for (int i = 0; i < myQPs.size(); i++)
        {
            verbs->QPStats.at(myQPs.at(i)) = arr + i;
        }
        if (verbs->DirectConnect == false)
        {
            //now deal with counetrs. Counters should also be packed per core.
            std::vector<int> myKeys;
            for (int i = 0; i < keySize.size(); i++)
            {
                //is this my key?
                auto wQP = verbs->workerKey2QPIdx[i];
                auto target = verbs->workerQP2SrvId[wQP];
                if (target == ps::Postoffice::Get()->my_rank())
                {
                    //put 0 there because the key determines the core in FreeConnect mode.
                    if (verbs->Helper_Server_GetEndpointFromKey(i, 0).CoreIdx == tid)
                    {
                        myKeys.push_back(i);
                    }
                }
            }
            //allocate many atomic counters.
            atomic_int *atomInts = (atomic_int *)AlignedAllocateUniversal(RoundUp(myKeys.size() * sizeof(atomic_int), CACHELINE_SIZE_BYTES), mySock);
            CHECK(atomInts != NULL) << "Cannot allocate atomic int counters for keys on numa node " << mySock << ". Reduce key chunk?";
            //now put the pointers back into the array for easy access.
            for (int i = 0; i < myKeys.size(); i++)
            {
                UpdatesReceivedBuffer.at(myKeys.at(i)) = atomInts + i;
            }
        }
    }
    volatile bool GTG = false;
    void selfLoopMessage(int &selfLoopPollCntr, ibv_send_wr *wr, ibv_qp *selfLoopQP, ibv_cq *selfLoopSCQ)
    {
        ibv_send_wr *garbage = NULL;
        CHECK(selfLoopPollCntr > 0);
        if (--selfLoopPollCntr == 0)
        {
            selfLoopPollCntr = Verbs::send_queue_depth;
            wr->send_flags = IBV_SEND_SIGNALED;
            CHECK(ibv_post_send(selfLoopQP, wr, &garbage) == 0);
            ibv_wc selfLoopWC;
            while (0 == ibv_poll_cq(selfLoopSCQ, 1, &selfLoopWC))
                ;
            //raise(SIGTRAP);

            CHECK(selfLoopWC.status == IBV_WC_SUCCESS) << ibv_wc_status_str(selfLoopWC.status);
            //workerQP2SrvQPI/<< " key=" << j;
        }
        else
        {
            wr->send_flags = 0;
            CHECK(ibv_post_send(selfLoopQP, wr, &garbage) == 0);
            //printf("queueing myself. tid = %d, key = %d \n", tid, j);
        }
    }
    void infiniBandCollect(size_t tid)
    {
        while (GTG == false)
            ;
        //check my Numa assignment is in sane.
        CHECK(GetCurrentThreadNumaNode() == verbs->Core2SocketIdx.at(tid)) << "Current node = " << GetCurrentThreadNumaNode() << " expected = " << verbs->Core2SocketIdx.at(tid);
        CHECK(my_node_.role == Node::SERVER); //sanity check
        auto pollSize = verbs->core2CQIdxs.at(tid).size();
        if (pollSize == 0)
            return;
        PerCoreNUMAAwareInitialization(tid);
        //printf("[%d][%d]PSHUB data thread %d is up.\n", my_node_.id, my_node_.role, tid);
        auto workers = ps::Postoffice::Get()->GetNodeIDs(kWorkerGroup);
        uint64_t memoizedPoll = 0;
        //create a local copy to avoid verbs floating around.
        std::vector<int> pollVector = verbs->core2CQIdxs.at(tid);

        auto selfLoopDevId = verbs->CQ2DeviceIdx.at(verbs->core2CQIdxs.at(tid).at(0));
        //now, create a queue pair from this dev to this dev.
        //then, create some queue_pair
        auto selfLoopSCQ = ibv_create_cq(verbs->contexts.at(selfLoopDevId),
                                         Verbs::send_completion_queue_depth,
                                         NULL, // no user context
                                         NULL, // no completion channel
                                         0);   // no completion channel vector
        auto selfLoopRCQ = ibv_create_cq(verbs->contexts.at(selfLoopDevId),
                                         Verbs::recv_completion_queue_depth,
                                         NULL, // no user context
                                         NULL, // no completion channel
                                         0);

        ibv_qp_init_attr init_attributes;
        std::memset(&init_attributes, 0, sizeof(ibv_qp_init_attr));
        // use shared completion queue
        init_attributes.send_cq = selfLoopSCQ;
        init_attributes.recv_cq = selfLoopRCQ;
        //make sure we're using the same device.
        //CHECK(endpoints.at(i).DeviceIdx == CQ2DeviceIdx.at(cqIndex));
        // use "reliable connected" model in order to support RDMA atomics
        init_attributes.qp_type = IBV_QPT_RC;

        // only issue send completions if requested
        init_attributes.sq_sig_all = 0;

        // set queue depths and WR parameters accoring to constants declared earlier
        init_attributes.cap.max_send_wr = Verbs::send_queue_depth;
        init_attributes.cap.max_recv_wr = Verbs::receive_queue_depth;
        init_attributes.cap.max_send_sge = Verbs::scatter_gather_element_count;
        init_attributes.cap.max_recv_sge = Verbs::scatter_gather_element_count;
        init_attributes.cap.max_inline_data = Verbs::max_inline_data;
        // create queue pair
        //printf("[%d] devid = %d, cq = %d, ep = %d, sendCQ = %p, context = %p\n", MyID, devId, cqIndex, i, sendCompletionQueues.at(cqIndex), protection_domains.at(devId)->context);
        auto selfLoopQP = ibv_create_qp(verbs->protection_domains.at(selfLoopDevId), &init_attributes);
        //connect qp toself.
        ibv_qp_attr attributes;

        attributes.qp_state = IBV_QPS_INIT;
        attributes.port_num = verbs->ports.at(selfLoopDevId);
        attributes.pkey_index = 0;
        attributes.qp_access_flags = (IBV_ACCESS_LOCAL_WRITE |
                                      IBV_ACCESS_REMOTE_WRITE |
                                      IBV_ACCESS_REMOTE_READ |
                                      IBV_ACCESS_REMOTE_ATOMIC);
        int retval = ibv_modify_qp(selfLoopQP, &attributes,
                                   IBV_QP_STATE |
                                       IBV_QP_PKEY_INDEX |
                                       IBV_QP_PORT |
                                       IBV_QP_ACCESS_FLAGS);
        if (retval < 0)
        {
            perror("Error setting queue pair to INIT");
            exit(1);
        }

        // move to RTR
        std::memset(&attributes, 0, sizeof(attributes));
        attributes.qp_state = IBV_QPS_RTR;
        attributes.path_mtu = verbs->ports_attribute.at(selfLoopDevId).active_mtu;
        attributes.dest_qp_num = selfLoopQP->qp_num;
        attributes.rq_psn = 0;
        attributes.max_dest_rd_atomic = Verbs::max_dest_rd_atomic;
        attributes.min_rnr_timer = Verbs::min_rnr_timer;
        attributes.ah_attr.is_global = 0;
        attributes.ah_attr.dlid = verbs->ports_attribute.at(selfLoopDevId).lid;
        attributes.ah_attr.sl = 0;
        attributes.ah_attr.src_path_bits = 0;
        attributes.ah_attr.port_num = verbs->ports.at(selfLoopDevId);
        retval = ibv_modify_qp(selfLoopQP, &attributes,
                               IBV_QP_STATE |
                                   IBV_QP_AV |
                                   IBV_QP_PATH_MTU |
                                   IBV_QP_DEST_QPN |
                                   IBV_QP_RQ_PSN |
                                   IBV_QP_MAX_DEST_RD_ATOMIC |
                                   IBV_QP_MIN_RNR_TIMER);
        if (retval < 0)
        {
            perror("Error setting queue pair to RTR");
            exit(1);
        }
        //printf("[%d]ibv_modify_qp[%d] phase2\n", thisVan->my_node().id, i);
        // move to RTS
        std::memset(&attributes, 0, sizeof(attributes));
        attributes.qp_state = IBV_QPS_RTS;
        attributes.timeout = Verbs::timeout;
        attributes.retry_cnt = Verbs::retry_count;
        attributes.rnr_retry = Verbs::rnr_retry;
        attributes.sq_psn = 0;
        attributes.max_rd_atomic = Verbs::max_rd_atomic;
        retval = ibv_modify_qp(selfLoopQP, &attributes,
                               IBV_QP_STATE |
                                   IBV_QP_TIMEOUT |
                                   IBV_QP_RETRY_CNT |
                                   IBV_QP_RNR_RETRY |
                                   IBV_QP_SQ_PSN |
                                   IBV_QP_MAX_QP_RD_ATOMIC);
        if (retval < 0)
        {
            perror("Error setting queue pair to RTS");
            exit(1);
        }
        auto kCount = keySize.size();
        std::vector<ibv_send_wr> selfLoopSendWR(kCount);
        std::vector<ibv_recv_wr> selfLoopRecvWR(kCount);
        std::vector<ibv_sge> selfLoopSGE(kCount);
        auto selfLoopSocketId = verbs->Device2SocketIdx.at(selfLoopDevId);
        //size_t selfLoopLen;
        //auto selfLoopAddr = PHubAllocator::Get()->PHUBMergeKVBuffer(selfLoopSocketId, selfLoopLen);
        for (int i = 0; i < kCount; i++)
        {
            size_t len;
            auto rAddr = PHubAllocator::Get()->PHUBMergeKVBuffer(i, 0, verbs->Device2SocketIdx.at(selfLoopDevId), len);
            //now deal with SGEs.
            selfLoopSGE.at(i).addr = (uint64_t)rAddr;
            selfLoopSGE.at(i).length = len;
            selfLoopSGE.at(i).lkey = verbs->DeviceMemoryRegions.at(selfLoopDevId)->lkey;

            selfLoopSendWR.at(i).wr_id = i;
            selfLoopSendWR.at(i).num_sge = 1;
            selfLoopSendWR.at(i).sg_list = &selfLoopSGE[i];
            selfLoopSendWR.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            selfLoopSendWR.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | i;

            selfLoopSendWR.at(i).wr.rdma.remote_addr = (uint64_t)rAddr;
            selfLoopSendWR.at(i).wr.rdma.rkey = verbs->DeviceMemoryRegions.at(selfLoopDevId)->rkey;

            //receive requests.
            selfLoopRecvWR.at(i).wr_id = i;
            verbs->post_receive_raw(selfLoopQP, &selfLoopRecvWR.at(i));
        }
        int selfLoopPollCntr = Verbs::send_queue_depth;
        //pollVector.push_back();
        while (stopRequested == false)
        {
            ibv_wc wc;
            size_t cqIdx = pollVector.at(memoizedPoll++ % pollSize);
            bool resurrectedDueToSelfLoopPoll = false;
            /*if (0 == verbs->poll(1, cqIdx, Verbs::CompletionQueueType::Receive, &wc))
            {
                resurrectedDueToSelfLoopPoll = true;
                if (0 == ibv_poll_cq(selfLoopRCQ, 1, &wc))
                {
                    continue;
                }
            }*/

            if (0 == ibv_poll_cq(selfLoopRCQ, 1, &wc))
            {
                if (0 == verbs->poll(1, cqIdx, Verbs::CompletionQueueType::Receive, &wc))
                {
                    continue;
                }
            }
            CHECK(wc.status == IBV_WC_SUCCESS) << "Caught incorrect status " << ibv_wc_status_str(wc.status) << " additional information: coming from selfloop=" << resurrectedDueToSelfLoopPoll;
            //sleep(0);
            //poll the infiniband connection and see whats going on.
            //pull key j.
            //printf("[%d]Polling Completion Queue id = %d\n",msg->meta.recver,j);
            auto sender = (wc.imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK;

            auto j = wc.imm_data & PHUB_IMM_KEY_MASK;
            if (sender == my_node().id)
            {
                //do a vector vector add.
                //a fake one.
                //no direct connect.
                int sid = verbs->Helper_Server_GetEndpointFromKey(j, 0).SocketIdx;
                if (SuppressAggregator == false)
                {
                    ADD->VectorVectorAdd((float *)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(),
                                         PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE,
                                         (float *)PerSocketMergeBuffers[sid][j].GetCurrentReadBuffer());
                }
                //add one.
                selfLoopKeyCounter.at(j)++;
                CHECK(selfLoopKeyCounter.at(j) <= selfLoopExpectedCounter);
                if (selfLoopKeyCounter.at(j) == selfLoopExpectedCounter)
                {
                    if (OPT != NULL && SuppressOptimizer == false)
                    {
                        OPT->Update(j,
                                    (float *)PerSocketMergeBuffers[sid][j].GetCurrentReadBuffer(),
                                    (float *)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(),
                                    PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE);
                    }
                    //we have flipped the buffer. We're clear to send out fast acks.
                    //we dont need to clear up timestamps before pushing, because this thread is in control.
                    for (auto it = 0; it < workers.size(); it++)
                    {
                        MetaSlim *meta = psSendBuffer[it][j].pMetaSlimBuffer;
                        //printf("[PSHuB] pushing out meta = %llx\n", meta);
                        auto socketId = verbs->Helper_Server_GetEndpointFromKey(j, it).SocketIdx;
                        //elided pulls
                        PerSocketMergeBuffers[socketId][j].PostSendBufferBundledPushPullAck(it);
                    }
                    selfLoopKeyCounter.at(j) = 0;
                }
                else
                {
                    //loop again.
                    selfLoopMessage(selfLoopPollCntr, &selfLoopSendWR.at(j), selfLoopQP, selfLoopSCQ);
                }
                verbs->post_receive_raw(selfLoopQP, &selfLoopRecvWR.at(j));
                continue;
            }
            auto mit = node2MachineIdx.at(sender);
            CHECK(mit != -1) << " no route to " << sender;
            auto i = mit;
            //printf("[%d][PRE]PSHUB received key=%d from=%d len=%d more=%d, updatesreceived = %d, workersize = %d\n", dbgMyId, j, sender, wc.byte_len, UpdatesReceivedBuffer[j].core.load() - workers.size(), UpdatesReceivedBuffer[j].core.load(), workers.size());

            //printf("[%d][%d]Posting RecvBuffer[%d][%d]. Msg from %d. Size=%d\n",dbgMyId, my_node().role,i,j,wc.imm_data, wc.byte_len);
            ReceiveRequests[wc.wr_id]->PostReceiveRequest();
            //printf("[%d][%d]RECVDBG byte_len=%d\n", dbgMyId, my_node_.role,wc.byte_len);
            auto &buffer = psRecvBuffer[i][j];
            auto sid = verbs->Helper_Server_GetEndpointFromKey(j, i).SocketIdx;
            if (SuppressAggregator == false)
            {
                //this is fine because in a socket only one processor can see this key.
                //so even though all of them sees 0, they can copy no problem.
                if (UpdatesReceivedBuffer.at(j)->load() == 0)
                {
                    //copy. in fact we can do this for each socket, but unfortunately UpdatesReceivedBuffer is per key, not per socket per key.
                    memcpy(PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(), buffer.pKVBuffer, keySize[j]);
                }
                else
                {
                    //CHECK(neata.size() == keySize[j] / sizeof(float)) << "newData = "<<newData.size() << " bytes vs " << keySize[j] / sizeof(float);
                    //CHECK(newData.size() == MergeBuffers[j].size());
                    auto mergeBuffer = (float *)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer();
                    //lets play with SSE for a bit.

                    ADD->VectorVectorAdd((float *)mergeBuffer, PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE, (float *)buffer.pKVBuffer);
                }

                MEASURE_THIS_TIME_END(PERFMON_PHUB_KEY_MERGE, j);
            }
            //this one has payload
            //this cannot be done alone. use rmw to test for value.
            //UpdatesReceivedBuffer[j].core++;
            //printf("[%d]PSHUB received key=%d from=%d len=%d more=%d, updatesreceived = %d, workersize = %d\n", dbgMyId, j, sender, wc.byte_len, UpdatesReceivedBuffer[j].core.load() - workers.size(), UpdatesReceivedBuffer[j].core.load(), workers.size());
            //to enable asynchronous mode, comment out the if statement.
            if (IsAsync || UpdatesReceivedBuffer[j]->fetch_add(1, std::memory_order_relaxed) + 1 == workers.size())
            {
                //DirectConnect requires reset this counter BEFORE the first BundledPush/Pull ack is sent out.
                //So that no signal is lost.
                UpdatesReceivedBuffer[j]->store(0, std::memory_order_relaxed); //bye bye. safe because im still listening to the key
                //send myself a message.
                selfLoopMessage(selfLoopPollCntr, &selfLoopSendWR.at(j), selfLoopQP, selfLoopSCQ);
                continue;
                //Do optimization here.
            }
            //printf("[%d][post]PSHUB received key=%d from=%d len=%d more=%d, updatesreceived = %d, workersize = %d\n", dbgMyId, j, sender, wc.byte_len, UpdatesReceivedBuffer[j].core.load() - workers.size(), UpdatesReceivedBuffer[j].core.load(), workers.size());
        }
        printf("[%d][%d]PSHUB data thread shutting down\n", my_node_.id, my_node_.role);
    }

  protected:
    virtual int GetUnderlyingWorkerThreadCount() override
    {
        if (my_node_.role != Node::SERVER)
        {
            return 1;
        }
        else
        {
            //if server
            return MaximumAllowedThreads;
        }
    }
};
} // namespace ps
