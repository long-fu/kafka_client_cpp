cmake_minimum_required(VERSION 3.0.0)
project(producer_client VERSION 0.1.0 LANGUAGES C CXX)

include(CTest)
enable_testing()

include_directories(
    ../common/include/
    ../common/
    inc/
)

add_executable(producer
    main.cpp
    src/producer.cpp)

target_link_libraries(producer PUBLIC
    ${CMAKE_SOURCE_DIR}/common/lib/libglog.so.0.6.0
    ${CMAKE_SOURCE_DIR}/common/lib/librdkafka++.so.1
    ${CMAKE_SOURCE_DIR}/common/lib/librdkafka.so.1
)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)