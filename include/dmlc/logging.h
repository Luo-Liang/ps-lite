/*!
 *  Copyright (c) 2015 by Contributors
 * \file logging.h
 * \brief defines logging macros of dmlc
 *  allows use of GLOG, fall back to internal
 *  implementation when disabled
 */
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <stdexcept>
#include "./base.h"
#include "./DIME.h"
#include <string.h>
#include <fcntl.h>
#include <signal.h>

#if defined(_MSC_VER) && _MSC_VER < 1900
#define noexcept(a)
#endif


 // use a light version of glog
#include <assert.h>
#include <iostream>
#include <sstream>
#include <ctime>
#include "../ps/internal/ext.h"
#include "../ps/rendezvous.h"

#if defined(_MSC_VER)
#pragma warning(disable : 4722)
#endif
extern std::string LoggingRendezvousIP;
extern uint LoggingRendezvousPort;
extern NodeId ID;
inline void InitLogging(std::string loggingUrl, NodeId id) {
	// DO NOTHING
	auto redisVec = CxxxxStringSplit(loggingUrl, ':');
	assert(redisVec.size() == 2);
	var redisAddr = redisVec[0];
	var redisPort = atoi(redisVec[1].c_str());
	assert(redisPort != -1);
	LoggingRendezvousIP = redisAddr;
	LoggingRendezvousPort = redisPort;
	ID = id;
}

static std::string trap()
{
	while (0)
	{
		__asm__("");
	}

	return "Turn on infinite loop to allow debugger to be attched.";
}

// Always-on checking
#define CHECK(x)				\
    if (!(x))							    \
    LogMessageFatal(__FILE__, __LINE__).stream() << "Check "  \
	"failed: " #x <<trap()

class DateLogger {
public:
	DateLogger() {
#if defined(_MSC_VER)
		_tzset();
#endif
	}
	const char* HumanDate() {
#if defined(_MSC_VER)
		_strtime_s(buffer_, sizeof(buffer_));
#else
		time_t time_value = time(NULL);
		struct tm now;
		localtime_r(&time_value, &now);
		snprintf(buffer_, sizeof(buffer_), "%02d:%02d:%02d", now.tm_hour,
			now.tm_min, now.tm_sec);
#endif
		return buffer_;
	}
private:
	char buffer_[9];
};
class LogMessageFatal {
public:
	LogMessageFatal(const char* file, int line) {
		//   if(IsDebuggerPresent())
		//{
		printf("[CRITICAL ERROR] %s\n", file);
		raise(SIGTRAP);

		//}
		log_stream_ << "[" << pretty_date_.HumanDate() << "] " << file << ":"
			<< line << ": ";
	}
	std::ostringstream &stream() { return log_stream_; }
	~LogMessageFatal()  {
		// throwing out of destructor is evil
		// hopefully we can do it here
		// also log the message before throw

		//send to redis
		Rendezvous(LoggingRendezvousIP, LoggingRendezvousPort, ID);
		//everything is dying why do you care
		raise(SIGTRAP);
	}


private:
	std::ostringstream log_stream_;
	DateLogger pretty_date_;
	LogMessageFatal(const LogMessageFatal&);
	void operator=(const LogMessageFatal&);
};
