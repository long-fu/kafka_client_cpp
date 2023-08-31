#include <iostream>
#include <csignal>

#include "utils.h"
// ExecStart=alarm_send localhost:9092 alarm-events console-consumer-62320 logs_dir
int consumer(std::string brokers, std::string topic_str, std::string group_id);
int main(int argc, char const *argv[])
{
    std::string lod_dir = "../../logs";
    LOG_INIT(argv[0], lod_dir);
    return consumer("localhost:9092","alarm-events","console-consumer-62311");
}
