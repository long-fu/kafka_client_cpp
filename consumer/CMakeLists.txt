cmake_minimum_required(VERSION 3.0.0)
project(consumer_client VERSION 0.1.0 LANGUAGES C CXX)

include(CTest)
enable_testing()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

include_directories(
    ../common/include/
    ../common/
    inc/
)

add_executable(consumer
    main.cpp
    src/consumer.cpp
    src/http.cpp
    ../common/utils.cpp
)

target_link_libraries(consumer PUBLIC
    ${CMAKE_SOURCE_DIR}/common/lib/libglog.so.0.6.0
    ${CMAKE_SOURCE_DIR}/common/lib/liboatpp.so
    ${CMAKE_SOURCE_DIR}/common/lib/librdkafka++.so.1
    ${CMAKE_SOURCE_DIR}/common/lib/librdkafka.so.1
)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)