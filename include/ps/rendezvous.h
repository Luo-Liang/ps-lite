#pragma once
#include <hiredis/hiredis.h>
#include <string>
#include <memory>
#include "phub.h"
#include <unistd.h>
using namespace std;
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

};