#include <iostream>
#include <oatpp/web/client/HttpRequestExecutor.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/network/tcp/client/ConnectionProvider.hpp>
#include <oatpp/web/client/ApiClient.hpp>
#include <oatpp/core/macro/codegen.hpp>

#include "utils.h"

using namespace oatpp::network;
using namespace oatpp::web;
using namespace oatpp::parser;

class ConsumerApiClient : public oatpp::web::client::ApiClient
{
#include OATPP_CODEGEN_BEGIN(ApiClient)

    API_CLIENT_INIT(ConsumerApiClient)

    //-----------------------------------------------------------------------------------------------
    // Synchronous calls
    API_CALL("POST", "api/smartbox/AlarmPost", alarmPost, BODY_STRING(String, body))

#include OATPP_CODEGEN_END(ApiClient)
};

static std::shared_ptr<oatpp::web::client::RequestExecutor> createOatppExecutor(const oatpp::network::Address &address)
{
    auto connectionProvider = oatpp::network::tcp::client::ConnectionProvider::createShared(address);
    return oatpp::web::client::HttpRequestExecutor::createShared(connectionProvider);
}

static int run_http_request(const std::string &host, const v_uint16 port, std::string body)
{

    /* Create ObjectMapper for serialization of DTOs  */
    auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared();

    /* Create RequestExecutor which will execute ApiClient's requests */
    auto requestExecutor = createOatppExecutor({host, port}); // <-- Always use oatpp native executor where's possible.
    // auto requestExecutor = createCurlExecutor();  // <-- Curl request executor

    /* DemoApiClient uses DemoRequestExecutor and json::mapping::ObjectMapper */
    /* ObjectMapper passed here is used for serialization of outgoing DTOs */
    auto client = ConsumerApiClient::createShared(requestExecutor, objectMapper);

    auto data = client->alarmPost(body)->readBodyToString();

    // TODO: 处理结果

    // OATPP_LOGD("TAG", "[alarmPost] data='%s'", data->c_str());
    
    return 0;
}

// 1024 * 1024 * 32
#define BODY_BUF_SIZE 33554432
static char *g_body_buf = NULL;

int http_send(const char *msg, size_t msg_len)
{
    
    int ret = 0;

    if (msg == NULL || msg_len == 0)
    {
        return 0;
    }

    if (g_body_buf == NULL)
    {
        g_body_buf = (char *)malloc(BODY_BUF_SIZE);
    }

    char *cameraName = NULL;
    char *alarmTime = NULL;
    char *algCode = NULL;
    char *deviceId = NULL;
    char *alarmExtension = NULL;
    char *alarmBase = NULL;

    char *des_ip = NULL;
    int des_port = 0;
    char *temp_port = NULL;

    cameraName = strtok((char *)msg, "\r\n");
    if (cameraName == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 cameraName";
        return -1;
    }

    alarmTime = strtok(NULL, "\r\n");
    if (alarmTime == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 alarmTime";
        return -1;
    }

    algCode = strtok(NULL, "\r\n");
    if (algCode == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 algCode";
        return -1;
    }

    deviceId = strtok(NULL, "\r\n");
    if (deviceId == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 deviceId";
        return -1;
    }

    alarmExtension = strtok(NULL, "\r\n");
    if (alarmExtension == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 alarmExtension";
        return -1;
    }

    alarmBase = strtok(NULL, "\r\n");
    if (alarmBase == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 alarmBase";
        return -1;
    }

    des_ip = strtok(NULL, "\r\n");
    if (des_ip == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 des_ip";
        return -1;
    }

    temp_port = strtok(NULL, "\r\n");
    if (temp_port == NULL)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 temp_port";
        return -1;
    }

    des_port = atoi(temp_port);
    if (des_port == 0)
    {
        ERROR_LOG() << "[HTTP] 消息解析错误 des_port";
        return -1;
    }

    memset(g_body_buf, 0x0, BODY_BUF_SIZE);

    sprintf(g_body_buf, "{\"CameraName\":\"%s\",\"SiteData\":{\"Latitude\":\"16.24463,44.179439\",\"Longitude\":\"001\",\"Name\":\"001\"},\"ChannelName\":\"\",\"AlarmTime\":\"%s\",\"AlgCode\":\"%s\",\"DeviceId\":\"%s\",\"AlarmBoxs\":[{\"X\":1236,\"Y\":545,\"Height\":529,\"Width\":234},{\"X\":1419,\"Y\":509,\"Height\":337,\"Width\":126},{\"X\":1203,\"Y\":545,\"Height\":388,\"Width\":123}],\"AlarmExtension\":\"%s\",\"ChannelId\":\"eb5d32\",\"AlarmBase\":\"%s\"}",
            cameraName, alarmTime, algCode, deviceId, alarmExtension, alarmBase);

    INFO_LOG() << "[HTTP] Msg body (" << strlen(g_body_buf) << ")";
    INFO_LOG() << "[HTTP] Host: " << des_ip << ":" << des_port;

    return run_http_request(des_ip, des_port, g_body_buf);
}
