/**
 *  Copyright (c) 2015 by Contributors
 */
#include "ps/internal/van.h"
#include <thread>
#include <chrono>
#include "ps/base.h"
#include "ps/sarray.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/customer.h"
#include "./network_utils.h"
#include "./meta.pb.h"
#include "./zmq_van.h"
#include "./infiniband_van.h"
#include "./resender.h"
#include "./pshub_van.h"
#include <sys/syscall.h>
#include "dmlc/DIME.h"
#include <fstream>
#include "PHubAllocator.h"
#include "GlooVan.h"

//TODO:: move these things away
#ifdef PHUB_PERF_DIAG
PerfMonLite PerfMonLite::e;
#endif
PHubAllocator PHubAllocator::INSTANCE; //the PHUB allocator.
//#if MXNET_USE_PROFILER
//mxnet::engine::Profiler* mxnet::engine::Profiler::instance_ = new mxnet::engine::Profiler();
//#else
//mxnet::engine::Profiler* mxnet::engine::Profiler::instance_ = nullptr;
//#endif

namespace ps {
#ifdef PHUB_PERF_DIAG
	int VanDecodeKey(ps::Key key) {
		auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[Postoffice::Get()->my_rank()];
		return key - kr.begin();
	}

	int VanEncodeKey(ps::Key key) {
		auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[Postoffice::Get()->my_rank()];
		return key + kr.begin();
	}
#endif
	
	Van* Van::Create(const std::string& type) {
	    char hostname[1024];
	    gethostname(hostname, 1024);

	    LOG(WARNING) << type << " selected. MID: " << hostname << " PID: " <<getpid() ;

#ifdef PHUB_PERF_DIAG
		PerfMonLite::Init();
#endif
		if (type == "zmq") {
			return new ZMQVan();
		}
		else if (type == "infiniband")
		{
			return new InfiniBandVan();
		}
		else if (type == "pshub")
		{
			auto tCounts = Environment::Get()->find("PSHUB_DATA_THREAD_COUNT");
			if (tCounts == NULL)
			{
				tCounts = "1";
			}
			auto tCounti = atoi(tCounts);
			return new PSHUBVan(tCounti);
		}
		else if (type == "gloo")
		{
		    return new GlooVan();
		}
		else {
			LOG(FATAL) << "unsupported van type: " << type;
			return nullptr;
		}

	}



	void Van::Start() {
		// get scheduler info
		scheduler_.hostname = std::string(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
		scheduler_.port = atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
		scheduler_.role = Node::SCHEDULER;
		scheduler_.id = kScheduler;
		is_scheduler_ = Postoffice::Get()->is_scheduler();

		// get my node info
		if (is_scheduler_) {
			my_node_ = scheduler_;
		}
		else {
			auto role = is_scheduler_ ? Node::SCHEDULER :
				(Postoffice::Get()->is_worker() ? Node::WORKER : Node::SERVER);
			const char* nhost = Environment::Get()->find("DMLC_NODE_HOST");
			std::string ip;
			if (nhost) ip = std::string(nhost);
			if (ip.empty()) {
				const char*  itf = Environment::Get()->find("DMLC_INTERFACE");
				std::string interface;
				if (itf) interface = std::string(itf);
				if (interface.size()) {
					GetIP(interface, &ip);
				}
				else {
					GetAvailableInterfaceAndIP(&interface, &ip);
				}
				CHECK(!interface.empty()) << "failed to get the interface";
			}
			int port = GetAvailablePort();
			const char* pstr = Environment::Get()->find("PORT");
			if (pstr) port = atoi(pstr);
			CHECK(!ip.empty()) << "failed to get ip";
			CHECK(port) << "failed to get a port";
			my_node_.hostname = ip;
			my_node_.role = role;
			my_node_.port = port;
			// cannot determine my id now, the scheduler will assign it later
			// set it explicitly to make re-register within a same process possible
			my_node_.id = Node::kEmpty;
		}
		//printf("[unknown][%d]Van half started\n", my_node_.role);
		// bind.
		my_node_.port = Bind(my_node_, is_scheduler_ ? 0 : 40);
		PS_VLOG(1) << "Bind to " << my_node_.DebugString();
		CHECK_NE(my_node_.port, -1) << "bind failed";

		// connect to the scheduler
		Connect(scheduler_);

		// for debug use
		if (Environment::Get()->find("PS_DROP_MSG")) {
			drop_rate_ = atoi(Environment::Get()->find("PS_DROP_MSG"));
		}
		// start receiver
		receiver_thread_ = std::unique_ptr<std::thread>(
			new std::thread(&Van::Receiving, this));

		//receiver_thread_1 = std::unique_ptr<std::thread>(
		//    new std::thread(&Van::Receiving, this));

		if (!is_scheduler_) {
			// let the scheduler know myself
			Message msg;
			msg.meta.recver = kScheduler;
			msg.meta.control.cmd = Control::ADD_NODE;
			msg.meta.control.node.push_back(my_node_);
			msg.meta.timestamp = timestamp_++;
			Send(msg);
		}
		// wait until ready
		while (!ready_) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}

		//PS_VLOG(2) << " node " << my_node_.id << " ready.";

		// resender
		if (Environment::Get()->find("PS_RESEND") && atoi(Environment::Get()->find("PS_RESEND")) != 0) {
			int timeout = 1000;
			if (Environment::Get()->find("PS_RESEND_TIMEOUT")) {
				timeout = atoi(Environment::Get()->find("PS_RESEND_TIMEOUT"));
			}
			resender_ = new Resender(timeout, 10, this);
		}

		if (!is_scheduler_ && (Environment::Get()->find("DISABLE_HEARTBEAT") == NULL || strcmp(Environment::Get()->find("DISABLE_HEARTBEAT"), "TRUE") != 0)) {
			// start heartbeat thread
			heartbeat_thread_ = std::unique_ptr<std::thread>(
				new std::thread(&Van::Heartbeat, this));
		}
		//printf("[%d][%d] Van fully started.\n", my_node_.id, my_node_.role);
	}

	void Van::Stop() {
		// stop threads
		Message exit;
		exit.meta.control.cmd = Control::TERMINATE;
		exit.meta.recver = my_node_.id;
		auto myid = my_node_.id;
		std::stringstream fName;
		fName << "PERFDIAG-" << my_node_.role << "-id-" <<myid << VanType();

#ifdef PHUB_PERF_DIAG
		std::ofstream PERF_LOG;
		PERF_LOG.open(fName.str().c_str(), std::ios_base::out);
		PERF_LOG << PerfMonLite::Get().Summarize(PerfMonLite::SummaryMode::Aggregated, PerfMonLite::OutputFormat::CSV) << std::endl;
		//PERF_LOG << "-------------DETAILS-------------" << std::endl;
		//PERF_LOG << PerfMonLite::Get().Summarize(PerfMonLite::SummaryMode::Full) << std::endl;
#endif
		fName << ".json";

		//mxnet::engine::Profiler::Get()->DUMP(fName.str());

		//printf("[%d]Stopping Van\n",myid);
		SendMsg(exit);
		//printf("[%d]Stop Message sent\n", myid);
		receiver_thread_->join();
		//printf("[%d]receive message joined\n",myid);
		if (!is_scheduler_ && heartbeat_thread_ != NULL) heartbeat_thread_->join();
		//printf("[%d]van has stopped\n", myid);
		if (resender_) delete resender_;
		//printf("[%d]resender has been deleted\n",myid);
	}

	int Van::Send(const Message& msg) {
		//static int cnter  = 0;
		/*  if(msg.meta.control.cmd == Control::BARRIER && my_node_.role == 2)
			{
			auto group = msg.meta.control.barrier_group;
			auto it = dbgMsg[group].find(my_node_.id);
			CHECK(it == dbgMsg[group].end());
			printf("[%d][%d]sending %s\n",my_node_.id, my_node_.role, msg.DebugString().c_str());
			print_stacktrace();
			printf("%d hello\n",cnter++);
			}*/
#ifdef PHUB_PERF_DIAG
		if (msg.meta.control.empty() && msg.meta.simple_app == false && msg.meta.head == 0)
		{
			if (my_node_.role == Node::WORKER)
			{
				if (msg.meta.push == true)
				{
					//printf("not executing here?\n");
					MEASURE_THIS_TIME_END(PERFMON_WORKER_PUSH_PIPELINE, *((ps::Key*)msg.data[0].data()));
				}
				else
				{
					MEASURE_THIS_TIME_END(PERFMON_WORKER_PULL_PIPELINE, *((ps::Key*)msg.data[0].data()));
				}
			}
			else if (my_node_.role == Node::SERVER)
			{
				//server, if we have key, is a pull, otherwise, is a push ack
				if (msg.meta.push == true)
				{
					//nokey, just a push ack
					//push ack needs to use extended selector
					MEASURE_THIS_TIME_END_WITH_EXTSEQ(PERFMON_PHUB_PUSH_ACK_PIPELINE, msg.meta.control.msg_sig, msg.meta.recver);
					//revert your key
					const_cast<Message&>(msg).meta.control.msg_sig = VanEncodeKey(msg.meta.control.msg_sig);
				}
				else
				{
					//printf("[%d][%d]processing msg %s. k = %d\n", my_node_.id, 0, msg.DebugString().c_str(), *((ps::Key*)msg.data[0].data()));

					MEASURE_THIS_TIME_END_WITH_EXTSEQ(PERFMON_PHUB_PULL_ACK_PIPELINE, VanDecodeKey(*((ps::Key*)msg.data[0].data())), msg.meta.recver);
				}
			}
		}
#endif
		int send_bytes = SendMsg(msg);
		/*   if(msg.meta.control.cmd == Control::INFINIBANDXCHGLIDS)
			 {
			 PS_VLOG(1)<<"Delivery of Infiniband Exchange Lids done.";
			 }*/
		CHECK_NE(send_bytes, -1) << " ZMQ could not deliver package from " << my_node_.id << msg.DebugString();
		//printf("[%d] location of send bytes = %p, val = %llu\n", my_node_.id,&send_bytes_, send_bytes_.load());
		send_bytes_ += send_bytes;

		if (resender_) resender_->AddOutgoing(msg);
		if (Postoffice::Get()->verbose() >= 2) {
		    // printf("[%d][%d][SEND]%s\n", my_node_.id, my_node_.role, msg.DebugString().c_str());
   		    //const_cast<Message&>(msg).meta.sender = my_node_.id;

		    PS_VLOG(2) <<"["<<my_node_.id<<"][SEND]"<< msg.DebugString();
			//print_stacktrace();
		}

		/*if(my_node_.role == 2)
		  {
		  printf("scheduler returning from send\n");
		  }*/
		return send_bytes;
	}

	void Van::Receiving() {
		const char* heartbeat_timeout_val = Environment::Get()->find("PS_HEARTBEAT_TIMEOUT");
		const int heartbeat_timeout = heartbeat_timeout_val ? atoi(heartbeat_timeout_val) : 5;
		Meta nodes;  // for scheduler usage
		pid_t tid = syscall(SYS_gettid);

		while (true) {
			Message msg;
			//receiverLock.lock();
			std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
			int recv_bytes = RecvMsg(&msg);

#ifdef PHUB_PERF_DIAG
			if (msg.meta.control.empty() && msg.meta.simple_app == false && msg.meta.head == 0)
			{
				if (my_node_.role == Node::SERVER)
				{
					auto k = VanDecodeKey(*((ps::Key*)msg.data[0].data()));
					if (msg.meta.push)
					{
						MEASURE_THIS_TIME_BEG_WITH_EXTSEQ(PERFMON_PHUB_PUSH_ACK_PIPELINE, k, 0, msg.meta.sender);
						//need to somehow remember the key.
						//this is done through the additionalpayload field of kvmeta
					}
					else
					{
						//printf("[%d]msg is %s, decodedkey = %d\n",my_node_.id,msg.DebugString().c_str(), (int)k);

						MEASURE_THIS_TIME_BEG_WITH_EXTSEQ(PERFMON_PHUB_PULL_ACK_PIPELINE, k, 0, msg.meta.sender);
					}
				}
				else if (my_node_.role == Node::WORKER)
				{
					if (msg.meta.push)
					{
						//printf("receiving msg = %s. msg_sig = %d\n",msg.DebugString().c_str(), (int)msg.meta.control.msg_sig);

						//Since we lost key information, recover it from kvapps.
					    //msg_sig is already decoded
						MEASURE_THIS_TIME_BEG(PERFMON_WORKER_PUSH_ACK_PIPELINE, msg.meta.control.msg_sig);
					}
					else
					{
						MEASURE_THIS_TIME_BEG(PERFMON_WORKER_PULL_ACK_PIPELINE, *((ps::Key*)msg.data[0].data()));
					}
				}
			}
#endif
			//auto endNow =  std::chrono::system_clock::now();
			//if(recv_bytes!= -1 && msg.meta.control.empty() && my_node_.role == Node::SERVER)
			//{
			//  if(msg.data.size() != 0)
			//  {
			//printf("%d - %d\n", recv_bytes, msg.data[1].size());
			//double delayMicroSec = std::chrono::duration_cast<std::chrono::microseconds>(endNow - now).count();
			//DIME.DimeLogMetric4(msg.meta.sender, recv_bytes, delayMicroSec, recv_bytes / delayMicroSec);
			// }
			//}
			//receiverLock.unlock();
			// For debug, drop received message
			if (ready_ && drop_rate_ > 0) {
				unsigned seed = time(NULL) + my_node_.id;
				if (rand_r(&seed) % 100 < drop_rate_) {
					LOG(WARNING) << "Drop message " << msg.DebugString();
					continue;
				}
			}

			CHECK_NE(recv_bytes, -1);
			recv_bytes_ += recv_bytes;
			if (Postoffice::Get()->verbose() >= 2) {
			    //printf("[%d][%d][RECV]%s\n", my_node_.id, my_node_.role, msg.DebugString().c_str());
			    //msg.meta.sender = my_node_.id;
					    //			    msg.meta.sender=my_node_.id;
			    PS_VLOG(2) <<"["<<my_node_.id<<"][RECV]"<<  msg.DebugString();
			}
			//LOG(WARNING) << recv_bytes<< std::endl;

			// duplicated message
			//DIME.DimeLogMetric1(recv_bytes);

			if (resender_ && resender_->AddIncomming(msg))
			{
				LG << "Dup message " << msg.DebugString();

				//DIME.DimeLogMetric1(recv_bytes);
				continue;
			}

			if (!msg.meta.control.empty()) {
				// do some management
				auto& ctrl = msg.meta.control;
				if (ctrl.cmd == Control::TERMINATE) {
					PS_VLOG(1) << my_node_.ShortDebugString() << " is stopped";
					ready_ = false;
					break;
				}
				else if (ctrl.cmd == Control::ADD_NODE) {
					size_t num_nodes = Postoffice::Get()->num_servers() +
						Postoffice::Get()->num_workers();
					auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout);
					std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
					Meta recovery_nodes;  // store recovery nodes
					recovery_nodes.control.cmd = Control::ADD_NODE;
					// assign an id
					if (msg.meta.sender == Meta::kEmpty) {
						CHECK(is_scheduler_);
						CHECK_EQ(ctrl.node.size(), 1);
						if (nodes.control.node.size() < num_nodes) {
							nodes.control.node.push_back(ctrl.node[0]);
						}
						else {
							// some node dies and restarts
							CHECK(ready_);
							for (size_t i = 0; i < nodes.control.node.size() - 1; ++i) {
								const auto& node = nodes.control.node[i];
								if (dead_set.find(node.id) != dead_set.end() && node.role == ctrl.node[0].role) {
									auto& recovery_node = ctrl.node[0];
									// assign previous node id
									recovery_node.id = node.id;
									recovery_node.is_recovery = true;
									PS_VLOG(1) << "replace dead node " << node.DebugString()
										<< " by node " << recovery_node.DebugString();
									nodes.control.node[i] = recovery_node;
									recovery_nodes.control.node.push_back(recovery_node);
									break;
								}
							}
						}
					}

					// update my id
					for (size_t i = 0; i < ctrl.node.size(); ++i) {
						const auto& node = ctrl.node[i];
						if (my_node_.hostname == node.hostname &&
							my_node_.port == node.port) {
							my_node_ = node;
							std::string rank = std::to_string(Postoffice::IDtoRank(node.id));
#ifdef _MSC_VER
							_putenv_s("DMLC_RANK", rank.c_str());
#else
							setenv("DMLC_RANK", rank.c_str(), true);
#endif
						}
					}

					if (is_scheduler_) {
						time_t t = time(NULL);
						if (nodes.control.node.size() == num_nodes) {
							// sort the nodes according their ip and port,
							std::sort(nodes.control.node.begin(), nodes.control.node.end(),
								[](const Node& a, const Node& b) {
								return (a.hostname.compare(b.hostname) | (a.port < b.port)) > 0;
							});
							// assign node rank
							for (auto& node : nodes.control.node) {
								CHECK_EQ(node.id, Node::kEmpty);
								int id = node.role == Node::SERVER ?
									Postoffice::ServerRankToID(num_servers_) :
									Postoffice::WorkerRankToID(num_workers_);
								PS_VLOG(1) << "assign rank=" << id << " to node " << node.DebugString();
								node.id = id;
								Connect(node);
								if (node.role == Node::SERVER) ++num_servers_;
								if (node.role == Node::WORKER) ++num_workers_;
								Postoffice::Get()->UpdateHeartbeat(node.id, t);
							}
							nodes.control.node.push_back(my_node_);
							nodes.control.cmd = Control::ADD_NODE;
							Message back; back.meta = nodes;
							for (int r : Postoffice::Get()->GetNodeIDs(
								kWorkerGroup + kServerGroup)) {
								back.meta.recver = r;
								back.meta.timestamp = timestamp_++;
								Send(back);
							}
							PS_VLOG(1) << "the scheduler is connected to "
								<< num_workers_ << " workers and " << num_servers_ << " servers";
							ready_ = true;
						}
						else if (recovery_nodes.control.node.size() > 0) {
							// send back the recovery node
							CHECK_EQ(recovery_nodes.control.node.size(), 1);
							Connect(recovery_nodes.control.node[0]);
							Postoffice::Get()->UpdateHeartbeat(recovery_nodes.control.node[0].id, t);
							Message back;
							for (int r : Postoffice::Get()->GetNodeIDs(
								kWorkerGroup + kServerGroup)) {
								if (r != recovery_nodes.control.node[0].id
									&& dead_set.find(r) != dead_set.end()) {
									// do not try to send anything to dead node
									continue;
								}
								// only send recovery_node to nodes already exist
								// but send all nodes to the recovery_node
								back.meta = (r == recovery_nodes.control.node[0].id) ? nodes : recovery_nodes;
								back.meta.recver = r;
								back.meta.timestamp = timestamp_++;
								Send(back);
							}
						}
					}
					else {
						for (const auto& node : ctrl.node) {
							Connect(node);
							if (!node.is_recovery && node.role == Node::SERVER) ++num_servers_;
							if (!node.is_recovery && node.role == Node::WORKER) ++num_workers_;
						}
						PS_VLOG(1) << my_node_.ShortDebugString() << " is connected to others";
						ready_ = true;
					}
				}
				else if (ctrl.cmd == Control::BARRIER) {
					if (msg.meta.request) {
						if (barrier_count_.empty()) {
							barrier_count_.resize(8, 0);
						}
						int group = ctrl.barrier_group;
						++barrier_count_[group];
						auto p = dbgMsg[group].insert(msg.meta.sender);
						if (!p.second)
						{
							printf("[error]%d is already in the group of %d\n", msg.meta.sender, group);
						}
						PS_VLOG(1) << "Barrier count for " << group << " : " << barrier_count_[group] << " bdy=" << msg.meta.body << " from " << msg.meta.sender;
						if (barrier_count_[group] ==
							static_cast<int>(Postoffice::Get()->GetNodeIDs(group).size())) {
							barrier_count_[group] = 0;
							dbgMsg[group].clear();
							Message res;
							res.meta.request = false;
							res.meta.control.cmd = Control::BARRIER;
							for (int r : Postoffice::Get()->GetNodeIDs(group)) {
								res.meta.recver = r;
								res.meta.timestamp = timestamp_++;
								CHECK_GT(Send(res), 0);
							}
						}
					}
					else {
						Postoffice::Get()->Manage(msg);
					}
				}
				else if (ctrl.cmd == Control::HEARTBEAT) {
					time_t t = time(NULL);
					for (auto &node : ctrl.node) {
						Postoffice::Get()->UpdateHeartbeat(node.id, t);
						if (is_scheduler_) {
							Message heartbeat_ack;
							heartbeat_ack.meta.recver = node.id;
							heartbeat_ack.meta.control.cmd = Control::HEARTBEAT;
							heartbeat_ack.meta.control.node.push_back(my_node_);
							heartbeat_ack.meta.timestamp = timestamp_++;
							// send back heartbeat
							Send(heartbeat_ack);
						}
					}
				}
				else if (ctrl.cmd == Control::INFINIBANDXCHGQPS || ctrl.cmd == Control::INFINIBANDKEYSIZEEXCHG)
				{
					CHECK(my_node_.role == Node::SERVER || my_node_.role == Node::WORKER);
					HandleSpecialCommand(msg);
					//this one is a bit different.
					if (ctrl.cmd == Control::INFINIBANDKEYSIZEEXCHG)
					{
						//send an ack.
						Message keyExchgAck;
						keyExchgAck.meta.recver = msg.meta.sender;
						keyExchgAck.meta.sender = msg.meta.recver;
						keyExchgAck.meta.timestamp = msg.meta.timestamp;
						keyExchgAck.meta.customer_id = msg.meta.customer_id;
						keyExchgAck.meta.request = false;
						keyExchgAck.meta.push = msg.meta.push;
						keyExchgAck.meta.head = msg.meta.control.cmd;
						Send(keyExchgAck);
					}
				}
			}
			else
			{
				//a message is received. This message is not a control message.
				//timestamp it and see who gave this to me.
				//but only print something if im a server.

				CHECK_NE(msg.meta.sender, Meta::kEmpty);
				CHECK_NE(msg.meta.recver, Meta::kEmpty);
				CHECK_NE(msg.meta.customer_id, Meta::kEmpty);
				int id = msg.meta.customer_id;
				auto* obj = Postoffice::Get()->GetCustomer(id, 5);
				CHECK(obj) << "timeout (5 sec) to wait App " << id << "(" << my_node_.role << ") ready. msg disect=" << msg.DebugString();
				obj->Accept(msg);
				//printf("[%d][%d]delivered : %s\n",my_node_.id, my_node_.role, msg.DebugString().c_str());

				//if (Postoffice::Get()->verbose() >= 2) {
				//    PS_VLOG(2) <<"[DELIVERY CONFIMRATION]"<< msg.DebugString();
				//}
				//DIME.DimeLogMetric1(recv_bytes);
			}
		}
	}

	void Van::PackMeta(const Meta& meta, char** meta_buf, int* buf_size) {
		// convert into protobuf
		PBMeta pb;
		pb.set_head(meta.head);
		if (meta.customer_id != Meta::kEmpty) pb.set_customer_id(meta.customer_id);
		if (meta.timestamp != Meta::kEmpty) pb.set_timestamp(meta.timestamp);
		if (meta.body.size()) pb.set_body(meta.body);
		pb.set_push(meta.push);
		pb.set_request(meta.request);
		pb.set_simple_app(meta.simple_app);
		for (auto d : meta.data_type) pb.add_data_type(d);
#ifdef PHUB_PERF_DIAG
		if (true) {
#else
		if (!meta.control.empty()) {
#endif
			auto ctrl = pb.mutable_control();
			ctrl->set_cmd(meta.control.cmd);
			if (meta.control.cmd == Control::BARRIER) {
				ctrl->set_barrier_group(meta.control.barrier_group);
			}
#ifdef PHUB_PERF_DIAG
			ctrl->set_msg_sig(meta.control.msg_sig);
#else
			else if (meta.control.cmd == Control::ACK || meta.control.cmd == Control::INFINIBANDXCHGQPS) {
			    ctrl->set_msg_sig(meta.control.msg_sig); //usually a count
			    ctrl->set_barrier_group(meta.control.barrier_group); //usually a remote key
			}
#endif
			for (const auto& n : meta.control.node) {
				auto p = ctrl->add_node();
				p->set_id(n.id);
				p->set_role(n.role);
				p->set_port(n.port);
				p->set_hostname(n.hostname);
				p->set_is_recovery(n.is_recovery);
			}
		}
		// to string
		*buf_size = pb.ByteSize();
		*meta_buf = new char[*buf_size + 1];
		CHECK(pb.SerializeToArray(*meta_buf, *buf_size))
			<< "failed to serialize protbuf";
	}

	void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) {
		// to protobuf
		PBMeta pb;
		CHECK(pb.ParseFromArray(meta_buf, buf_size))
			<< "failed to parse string into protobuf";

		// to meta
		meta->head = pb.head();
		meta->customer_id = pb.has_customer_id() ? pb.customer_id() : Meta::kEmpty;
		meta->timestamp = pb.has_timestamp() ? pb.timestamp() : Meta::kEmpty;
		meta->request = pb.request();
		meta->push = pb.push();
		meta->simple_app = pb.simple_app();
		meta->body = pb.body();
		meta->data_type.resize(pb.data_type_size());
		for (int i = 0; i < pb.data_type_size(); ++i) {
			meta->data_type[i] = static_cast<DataType>(pb.data_type(i));
		}

		if (pb.has_control()) {
			const auto& ctrl = pb.control();
			meta->control.cmd = static_cast<Control::Command>(ctrl.cmd());
			meta->control.barrier_group = ctrl.barrier_group();
			meta->control.msg_sig = ctrl.msg_sig();
			for (int i = 0; i < ctrl.node_size(); ++i) {
				const auto& p = ctrl.node(i);
				Node n;
				n.role = static_cast<Node::Role>(p.role());
				n.port = p.port();
				n.hostname = p.hostname();
				n.id = p.has_id() ? p.id() : Node::kEmpty;
				n.is_recovery = p.is_recovery();
				meta->control.node.push_back(n);
			}
		}
		else {
			meta->control.cmd = Control::EMPTY;
		}
	}

	void Van::Heartbeat() {
		const char* val = Environment::Get()->find("PS_HEARTBEAT_INTERVAL");
		const int interval = val ? atoi(val) : 5;
		while (interval > 0 && ready_) {
			std::this_thread::sleep_for(std::chrono::seconds(interval));
			Message msg;
			msg.meta.recver = kScheduler;
			msg.meta.control.cmd = Control::HEARTBEAT;
			msg.meta.control.node.push_back(my_node_);
			msg.meta.timestamp = timestamp_++;
			Send(msg);
		}
	}
}  // namespace ps
