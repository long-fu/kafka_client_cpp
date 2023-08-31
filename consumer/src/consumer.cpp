#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#ifndef _WIN32
#include <sys/time.h>
#else
#include <windows.h> /* for GetLocalTime */
#endif

#ifdef _MSC_VER
#include "../win32/wingetopt.h"
#elif _AIX
#include <unistd.h>
#else
#include <getopt.h>
#include <unistd.h>
#endif

#include <librdkafka/rdkafkacpp.h>

#include "utils.h"

static bool exit_eof = false;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 1;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;

int http_send(const std::string &host, const uint16_t port, const std::string &body);

class ExampleEventCb : public RdKafka::EventCb
{
public:
    void event_cb(RdKafka::Event &event)
    {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
            if (event.fatal())
            {
                run = 0;
                FATAL_LOG() << "(" << RdKafka::err2str(event.err()) << "): " << event.str();
            } else {
                ERROR_LOG() << "(" << RdKafka::err2str(event.err()) << "): " << event.str();
            }
            break;

        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_LOG:
            INFO_LOG() << event.severity() << "-" << event.fac() << ": " << event.str();
            break;

        default:
            std::cerr << "EVENT " << event.type() << " ("
                      << RdKafka::err2str(event.err()) << "): " << event.str()
                      << std::endl;
            break;
        }
    }
};

/* Use of this partitioner is pretty pointless since no key is provided
 * in the produce() call. */
class MyHashPartitionerCb : public RdKafka::PartitionerCb
{
public:
    int32_t partitioner_cb(const RdKafka::Topic *topic,
                           const std::string *key,
                           int32_t partition_cnt,
                           void *msg_opaque)
    {
        return djb_hash(key->c_str(), key->size()) % partition_cnt;
    }

private:
    static inline unsigned int djb_hash(const char *str, size_t len)
    {
        unsigned int hash = 5381;
        for (size_t i = 0; i < len; i++)
            hash = ((hash << 5) + hash) + str[i];
        return hash;
    }
};

class ExampleRebalanceCb : public RdKafka::RebalanceCb
{
public:
    void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                      RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition *> &partitions)
    {


        std::string log_info = "RebalanceCb: " + RdKafka::err2str(err) + ": ";
        for (unsigned int i = 0; i < partitions.size(); i++)
            log_info = log_info + partitions[i]->topic() + "[" + std::to_string(partitions[i]->partition()) + "], ";
        log_info = log_info + "\r\n";

        INFO_LOG() << log_info;

        RdKafka::Error *error = NULL;
        RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;

        if (err == RdKafka::ERR__ASSIGN_PARTITIONS)
        {
            if (consumer->rebalance_protocol() == "COOPERATIVE")
                error = consumer->incremental_assign(partitions);
            else
                ret_err = consumer->assign(partitions);
            partition_cnt += (int)partitions.size();
        }
        else
        {
            if (consumer->rebalance_protocol() == "COOPERATIVE")
            {
                error = consumer->incremental_unassign(partitions);
                partition_cnt -= (int)partitions.size();
            }
            else
            {
                ret_err = consumer->unassign();
                partition_cnt = 0;
            }
        }
        eof_cnt = 0; /* FIXME: Won't work with COOPERATIVE */

        if (error)
        {
            ERROR_LOG() << "incremental assign failed: " << error->str();
            delete error;
        }
        else if (ret_err)
            ERROR_LOG() << "assign failed: " << RdKafka::err2str(ret_err);
    }
};

void msg_consume(RdKafka::Message *message, void *opaque)
{
    const RdKafka::Headers *headers;

    switch (message->err())
    {
    case RdKafka::ERR__TIMED_OUT:
        break;

    case RdKafka::ERR_NO_ERROR:
        /* Real message */
        INFO_LOG() << "Read msg at offset " << message->offset();
        if (message->key())
        {
            INFO_LOG() << "Key: " << *message->key();
        }
        headers = message->headers();
        if (headers)
        {
            std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
            for (size_t i = 0; i < hdrs.size(); i++)
            {
                const RdKafka::Headers::Header hdr = hdrs[i];

                if (hdr.value() != NULL)
                    printf(" Header: %s = \"%.*s\"\n", hdr.key().c_str(),
                           (int)hdr.value_size(), (const char *)hdr.value());
                else
                    printf(" Header:  %s = NULL\n", hdr.key().c_str());
            }
        }
        printf("%.*s\n", static_cast<int>(message->len()),
               static_cast<const char *>(message->payload()));
        break;

    case RdKafka::ERR__PARTITION_EOF:
        /* Last message */
        if (exit_eof)
        {
            run = 0;
        }
        break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
        ERROR_LOG() << "Consume failed: " << message->errstr();
        run = 0;
        break;

    default:
        /* Errors */
        ERROR_LOG() << "Consume failed: " << message->errstr();
        run = 0;
    }
}


int consumer(std::string brokers, std::string topic_str, std::string group_id)
{
    std::string errstr;
    std::vector<std::string> topics;

    /*
     * Create configuration objects
     */
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    ExampleRebalanceCb ex_rebalance_cb;
    conf->set("rebalance_cb", &ex_rebalance_cb, errstr);

    conf->set("enable.partition.eof", "true", errstr);

    if (conf->set("group.id", group_id, errstr) != RdKafka::Conf::CONF_OK)
    {
        ERROR_LOG() << errstr;
        exit(1);
    }

    if (conf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK)
    {
        ERROR_LOG() << errstr;
        exit(1);
    }

    // if (conf->set("compression.codec", optarg, errstr) !=
    //     RdKafka::Conf::CONF_OK)
    // {
    //     std::cerr << errstr << std::endl;
    //     exit(1);
    // }
    // if (conf->set("statistics.interval.ms", optarg, errstr) !=
    //     RdKafka::Conf::CONF_OK)
    // {
    //     std::cerr << errstr << std::endl;
    //     exit(1);
    // }

    topics.push_back(topic_str);

    /*
     * Set configuration properties
     */
    conf->set("metadata.broker.list", brokers, errstr);

    ExampleEventCb ex_event_cb;
    conf->set("event_cb", &ex_event_cb, errstr);

    /*
     * Consumer mode
     */

    /*
     * Create consumer using accumulated global configuration.
     */
    RdKafka::KafkaConsumer *consumer =
        RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer)
    {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;

    INFO_LOG() << "% Created consumer " << consumer->name();

    /*
     * Subscribe to topics
     */
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err)
    {
        FATAL_LOG() << "Failed to subscribe to " << topics.size()
                  << " topics: " << RdKafka::err2str(err);
        exit(1);
    }

    /*
     * Consume messages
     */
    while (run)
    {
        RdKafka::Message *msg = consumer->consume(1000);
        msg_consume(msg, NULL);
        delete msg;
    }

#ifndef _WIN32
    alarm(10);
#endif

    /*
     * Stop consumer
     */
    consumer->close();
    delete consumer;

    INFO_LOG() << "% Consumed " << msg_cnt << " messages (" << msg_bytes
              << " bytes)";

    /*
     * Wait for RdKafka to decommission.
     * This is not strictly needed (with check outq_len() above), but
     * allows RdKafka to clean up all its resources before the application
     * exits so that memory profilers such as valgrind wont complain about
     * memory leaks.
     */
    RdKafka::wait_destroyed(5000);
    return 0;
}
