#include <iostream>
#include <csignal>

#include "utils.h"

int main(int argc, char const *argv[])
{
    std::string lod_dir;
    LOG_INIT(argv[0], lod_dir);
    std::cout << "Hello, from kafka_client!\n";
}
