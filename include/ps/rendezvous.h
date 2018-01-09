#pragma once
#include <hiredis/hiredis.h>
#include <string>
#include <memory>
#include "phub.h"

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

	void Connect()
	{
		pContext = redisConnect(IP.c_str(), (int)Port);
		if (pContext == NULL || pContext->err)
		{
			CHECK
		}
	}

};