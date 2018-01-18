#pragma once
#include <hiredis/hiredis.h>
#include <string>
#include <memory>
#include "phub.h"
#include <unistd.h>
#include <json.hpp>
using namespace std;
using json = nlohmann::json;
class Rendezvous
{
	string IP;
	uint Port;
	redisContext* pContext;
public:
	Rendezvous(string ip, uint port) : IP(ip), Port(port)
	{

	}

	~Rendezvous()
	{
		redisFree(pContext);
		pContext = NULL;
	}

	void Connect()
	{
		pContext = redisConnect(IP.c_str(), (int)Port);
		CHECK(pContext != NULL);
		CHECK(pContext->err == NULL) << pContext->errstr;
		//clean up old dbs.
		CHECK(redisCommand(pContext, "FLUSHALL"));
	}

	void SynchronousBarrier(std::string name, int participants)
	{
		var reply = redisCommand(pContext, "INCR %s", name.c_str());
		CHECK(reply) << pContext->errstr;
		while (true)
		{
			usleep(50000);
			//try to see how many we have now.
			reply = redisCommand(pContext, "GET %s", name.c_str());
			CHECK(reply) << pContext->errstr;
			var pReply = (redisReply*)reply;
			CHECK(pReply->type == REDIS_REPLY_INTEGER);
			if (pReply->integer == participants)
			{
				break;
			}
		}
	}

	void PushMachineConfig(NodeId myId, MachineConfigDescriptor& config)
	{
		json j;
		j["sockets"] = config.SocketCount;
		j["core2socket"] = config.Core2SocketIdx;
		j["ibdevice2socket"] = config.ib_Device2SocketIdx;
		var reply = redisCommand(pContext, "SET mcds%d %s", myId,j.dump().c_str());
		CHECK(reply) << pContext->errstr;
	}

	MachineConfigDescSlim PullMachineConfig(NodeId myId)
	{
		var reply = redisCommand(pContext, "GET mcds%d", myId);
		CHECK(reply) << pContext->errstr;
		var pRep = (redisReply*)reply;
		var j = json::parse(pRep->str);
		MachineConfigDescSlim mcds;
		mcds.Cores2Socket = j["core2socket"].get<vector<int>>();
		mcds.Devices2Socket = j["ibdevice2socket"].get<vector<int>>();
		mcds.NumSockets = j["sockets"].get<int>();
		return mcds;
	}
};