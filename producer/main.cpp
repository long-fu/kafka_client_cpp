#include <iostream>
#include <csignal>

#include "utils.h"

int producer(std::string brokers, std::string topic, void *payload, size_t len);
int main(int argc, char const *argv[])
{
    return producer("localhost:9092","alarm-event",(void *)"mesg",4);
}
