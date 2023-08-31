#include "utils.h"


volatile sig_atomic_t run = 1;

void SignalHandle(const char *data, size_t size) {
    run = 0;
    std::string str = std::string(data, size);
    FATAL_LOG() << str;
}