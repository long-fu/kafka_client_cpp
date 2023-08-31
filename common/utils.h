#pragma once

#include <glog/logging.h>
#include <csignal>

extern volatile sig_atomic_t run;

extern "C" void SignalHandle(const char *data, size_t size);

#define LOG_INIT(X, Y)                     \
    FLAGS_colorlogtostderr = 1;            \
    FLAGS_alsologtostderr = 1;             \
    FLAGS_logbufsecs = 0;                  \
    FLAGS_max_log_size = 1024;             \
    FLAGS_log_dir = Y;                     \
    google::InitGoogleLogging(X);          \
    google::InstallFailureSignalHandler(); \
    google::InstallFailureWriter(&SignalHandle)

#define LOG_DEINIT() \
    google::ShutdownGoogleLogging()

#define INFO_LOG() LOG(INFO)
#define WARN_LOG() LOG(WARNING)
#define ERROR_LOG() LOG(ERROR)
#define FATAL_LOG() LOG(FATAL)
