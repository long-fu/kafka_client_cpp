/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2014-2022, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/confluentinc/librdkafka)
 */

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

static void metadata_print(const std::string &topic,
                           const RdKafka::Metadata *metadata)
{
    std::cout << "Metadata for " << (topic.empty() ? "" : "all topics")
              << "(from broker " << metadata->orig_broker_id() << ":"
              << metadata->orig_broker_name() << std::endl;

    /* Iterate brokers */
    std::cout << " " << metadata->brokers()->size() << " brokers:" << std::endl;
    RdKafka::Metadata::BrokerMetadataIterator ib;
    for (ib = metadata->brokers()->begin(); ib != metadata->brokers()->end();
         ++ib)
    {
        std::cout << "  broker " << (*ib)->id() << " at " << (*ib)->host() << ":"
                  << (*ib)->port() << std::endl;
    }
    /* Iterate topics */
    std::cout << metadata->topics()->size() << " topics:" << std::endl;
    RdKafka::Metadata::TopicMetadataIterator it;
    for (it = metadata->topics()->begin(); it != metadata->topics()->end();
         ++it)
    {
        std::cout << "  topic \"" << (*it)->topic() << "\" with "
                  << (*it)->partitions()->size() << " partitions:";

        if ((*it)->err() != RdKafka::ERR_NO_ERROR)
        {
            std::cout << " " << err2str((*it)->err());
            if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE)
                std::cout << " (try again)";
        }
        std::cout << std::endl;

        /* Iterate topic's partitions */
        RdKafka::TopicMetadata::PartitionMetadataIterator ip;
        for (ip = (*it)->partitions()->begin(); ip != (*it)->partitions()->end();
             ++ip)
        {
            std::cout << "    partition " << (*ip)->id() << ", leader "
                      << (*ip)->leader() << ", replicas: ";

            /* Iterate partition's replicas */
            RdKafka::PartitionMetadata::ReplicasIterator ir;
            for (ir = (*ip)->replicas()->begin(); ir != (*ip)->replicas()->end();
                 ++ir)
            {
                std::cout << (ir == (*ip)->replicas()->begin() ? "" : ",") << *ir;
            }

            /* Iterate partition's ISRs */
            std::cout << ", isrs: ";
            RdKafka::PartitionMetadata::ISRSIterator iis;
            for (iis = (*ip)->isrs()->begin(); iis != (*ip)->isrs()->end(); ++iis)
                std::cout << (iis == (*ip)->isrs()->begin() ? "" : ",") << *iis;

            if ((*ip)->err() != RdKafka::ERR_NO_ERROR)
                std::cout << ", " << RdKafka::err2str((*ip)->err()) << std::endl;
            else
                std::cout << std::endl;
        }
    }
}


static bool exit_eof = false;

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
    void dr_cb(RdKafka::Message &message)
    {
        std::string status_name;
        switch (message.status())
        {
        case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
            status_name = "NotPersisted";
            break;
        case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
            status_name = "PossiblyPersisted";
            break;
        case RdKafka::Message::MSG_STATUS_PERSISTED:
            status_name = "Persisted";
            break;
        default:
            status_name = "Unknown?";
            break;
        }
        std::cout << "Message delivery for (" << message.len()
                  << " bytes): " << status_name << ": " << message.errstr()
                  << std::endl;
        if (message.key())
            std::cout << "Key: " << *(message.key()) << ";" << std::endl;
    }
};

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
                std::cerr << "FATAL ";
                run = 0;
            }
            std::cerr << "ERROR (" << RdKafka::err2str(event.err())
                      << "): " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(),
                    event.str().c_str());
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


class ExampleRebalanceCb : public RdKafka::RebalanceCb {
 private:
  static void part_list_print(
      const std::vector<RdKafka::TopicPartition *> &partitions) {
    for (unsigned int i = 0; i < partitions.size(); i++)
      std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition()
                << "], ";
    std::cerr << "\n";
  }

 public:
  void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                    RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition *> &partitions) {
    std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

    part_list_print(partitions);

    RdKafka::Error *error      = NULL;
    RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      if (consumer->rebalance_protocol() == "COOPERATIVE")
        error = consumer->incremental_assign(partitions);
      else
        ret_err = consumer->assign(partitions);
      partition_cnt += (int)partitions.size();
    } else {
      if (consumer->rebalance_protocol() == "COOPERATIVE") {
        error = consumer->incremental_unassign(partitions);
        partition_cnt -= (int)partitions.size();
      } else {
        ret_err       = consumer->unassign();
        partition_cnt = 0;
      }
    }
    eof_cnt = 0; /* FIXME: Won't work with COOPERATIVE */

    if (error) {
      std::cerr << "incremental assign failed: " << error->str() << "\n";
      delete error;
    } else if (ret_err)
      std::cerr << "assign failed: " << RdKafka::err2str(ret_err) << "\n";
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
        std::cout << "Read msg at offset " << message->offset() << std::endl;
        if (message->key())
        {
            std::cout << "Key: " << *message->key() << std::endl;
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
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = 0;
        break;

    default:
        /* Errors */
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = 0;
    }
}

class ExampleConsumeCb : public RdKafka::ConsumeCb
{
public:
    void consume_cb(RdKafka::Message &msg, void *opaque)
    {
        msg_consume(&msg, opaque);
    }
};

int consumer(std::string brokers, std::string topic_str, std::string group_id)
{
    std::string errstr;
    int use_ccb = 0;
    int32_t partition = RdKafka::Topic::PARTITION_UA;
    int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;

    if (topic_str.empty())
        exit(1);

    /*
     * Create configuration objects
     */
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    /*
     * Consumer mode
     */

    conf->set("enable.partition.eof", "true", errstr);

    ExampleRebalanceCb ex_rebalance_cb;
    conf->set("rebalance_cb", &ex_rebalance_cb, errstr);

    ExampleEventCb ex_event_cb;
    conf->set("event_cb", &ex_event_cb, errstr);

    conf->set("metadata.broker.list", brokers, errstr);

    if (conf->set("group.id", group_id, errstr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    /*
     * Create consumer using accumulated global configuration.
     */
    RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
    if (!consumer)
    {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        exit(1);
    }

    std::cout << "% Created consumer " << consumer->name() << std::endl;

    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic =
        RdKafka::Topic::create(consumer, topic_str, tconf, errstr);
    if (!topic)
    {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        exit(1);
    }

    /*
     * Start consumer for topic+partition at start offset
     */
    RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
                  << std::endl;
        exit(1);
    }

    ExampleConsumeCb ex_consume_cb;

    /*
     * Consume messages
     */
    while (run)
    {
        if (use_ccb)
        {
            consumer->consume_callback(topic, partition, 1000, &ex_consume_cb,
                                       &use_ccb);
        }
        else
        {
            RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
            msg_consume(msg, NULL);
            delete msg;
        }
        consumer->poll(0);
    }

    /*
     * Stop consumer
     */
    consumer->stop(topic, partition);

    consumer->poll(1000);

    delete topic;
    delete consumer;
    return 0;
}
