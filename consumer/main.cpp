#include <iostream>
#include <csignal>

#include "utils.h"

#include "oatpp/web/client/HttpRequestExecutor.hpp"
#include "oatpp/network/tcp/client/ConnectionProvider.hpp"

#include "oatpp/parser/json/mapping/ObjectMapper.hpp"

int consumer(std::string brokers, std::string topic_str, std::string group_id);

int main(int argc, char const *argv[])
{
    std::string lod_dir = "../../logs";
    LOG_INIT(argv[0], lod_dir);

    
    oatpp::base::Environment::init();
    
    return consumer("localhost:9092","alarm-events","console-consumer-62311");

    oatpp::base::Environment::destroy();
    
    return 0;
}
