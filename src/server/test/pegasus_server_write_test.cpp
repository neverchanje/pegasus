// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"
#include "message_utils.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"
#include "server/pegasus_server_impl.h"
#include "base/pegasus_key_schema.h"

#include <dsn/utility/fail_point.h>
#include <dsn/utility/defer.h>

namespace pegasus {
namespace server {

class pegasus_server_write_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_server_write_test() : pegasus_server_test_base()
    {
        start();
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get(), true);
    }

    void test_batch_writes()
    {
        dsn::fail::setup();

        dsn::fail::cfg("db_write_batch_put", "10%return()");
        dsn::fail::cfg("db_write_batch_remove", "10%return()");
        dsn::fail::cfg("db_write", "10%return()");

        for (int decree = 1; decree <= 1000; decree++) {
            RPC_MOCKING(put_rpc) RPC_MOCKING(remove_rpc)
            {
                dsn::blob key;
                pegasus_generate_key(key, std::string("hash"), std::string("sort"));
                dsn::apps::update_request req;
                req.key = key;
                req.value.assign("value", 0, 5);

                int put_rpc_cnt = dsn::rand::next_u32(1, 10);
                int remove_rpc_cnt = dsn::rand::next_u32(1, 10);
                int total_rpc_cnt = put_rpc_cnt + remove_rpc_cnt;
                auto writes = new dsn::message_ex *[total_rpc_cnt];
                for (int i = 0; i < put_rpc_cnt; i++) {
                    writes[i] = pegasus::create_put_request(req);
                }
                for (int i = put_rpc_cnt; i < total_rpc_cnt; i++) {
                    writes[i] = pegasus::create_remove_request(key);
                }
                auto cleanup = dsn::defer([=]() { delete[] writes; });

                int err =
                    _server_write->on_batched_write_requests(writes, total_rpc_cnt, decree, 0);
                switch (err) {
                case FAIL_DB_WRITE_BATCH_PUT:
                case FAIL_DB_WRITE_BATCH_DELETE:
                case FAIL_DB_WRITE:
                case 0:
                    break;
                default:
                    ASSERT_TRUE(false) << "unacceptable error: " << err;
                }

                // make sure everything is cleanup after batch write.
                ASSERT_TRUE(_server_write->_put_rpc_batch.empty());
                ASSERT_TRUE(_server_write->_remove_rpc_batch.empty());
                ASSERT_TRUE(_server_write->_write_svc->_batch_qps_perfcounters.empty());
                ASSERT_TRUE(_server_write->_write_svc->_batch_latency_perfcounters.empty());
                ASSERT_EQ(_server_write->_write_svc->_batch_start_time, 0);
                ASSERT_EQ(_server_write->_write_svc->_impl->_batch.Count(), 0);
                ASSERT_EQ(_server_write->_write_svc->_impl->_update_responses.size(), 0);

                ASSERT_EQ(put_rpc::mail_box().size(), put_rpc_cnt);
                ASSERT_EQ(remove_rpc::mail_box().size(), remove_rpc_cnt);
                for (auto &rpc : put_rpc::mail_box()) {
                    verify_response(rpc.response(), err, decree);
                }
                for (auto &rpc : remove_rpc::mail_box()) {
                    verify_response(rpc.response(), err, decree);
                }
            }
        }

        dsn::fail::teardown();
    }

    void verify_response(const dsn::apps::update_response &response, int err, int64_t decree)
    {
        ASSERT_EQ(response.error, err);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
        ASSERT_EQ(response.server, _server_write->_write_svc->_impl->_primary_address);
    }
};

TEST_F(pegasus_server_write_test, batch_writes) { test_batch_writes(); }

struct malformed_write_test : public pegasus_server_write_test, testing::WithParamInterface<int>
{
};

TEST_P(malformed_write_test, handle_malformed_write)
{
    dsn::blob key;
    pegasus_generate_key(key, std::string("hash"), std::string("sort"));
    dsn::apps::update_request req;
    req.key = key;
    req.value.assign("value", 0, 5);

    for (int i = 0; i < 20; i++) {
        auto writes = new dsn::message_ex *[1];
        writes[0] = pegasus::create_put_request(req);
        auto cleanup = dsn::defer([=]() { delete[] writes; });

        dsn::message_ex *m = writes[0];
        dsn::blob req_body = m->buffers[m->buffers.size() - 1];
        for (int i = 0; i < 4; i++) { // inject 4-byte fault
            uint32_t pos = dsn::rand::next_u32(req_body.size());
            char c = static_cast<char>(dsn::rand::next_u32(128));
            const_cast<char *>(req_body.data())[pos] = c;
        }

        _server_write->on_batched_write_requests(writes, 1, 1001, 0);

        // flush
        rocksdb::Status status = _server->_db->Flush(rocksdb::FlushOptions{});
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(_server->last_durable_decree(), _server->last_durable_decree());
    }
}

INSTANTIATE_TEST_SUITE_P(random_malformed_write, malformed_write_test, testing::Range(1, 11));

} // namespace server
} // namespace pegasus
