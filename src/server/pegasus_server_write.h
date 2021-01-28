// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/replica_base.h>

#include "base/pegasus_rpc_types.h"
#include "pegasus_write_service.h"

namespace pegasus {
namespace server {

/// This class implements the interface of `pegasus_sever_impl::on_batched_write_requests`.
class pegasus_server_write : public dsn::replication::replica_base
{
public:
    pegasus_server_write(pegasus_server_impl *server, bool verbose_log);

    /// \return error code returned by rocksdb, i.e rocksdb::Status::code.
    /// **NOTE**
    /// Error returned is regarded as the failure of replica, thus will trigger
    /// cluster membership changes. Make sure no error is returned because of
    /// invalid user argument.
    /// As long as the returned error is 0, the operation is guaranteed to be
    /// successfully applied into rocksdb, which means an empty_put will be called
    /// even if there's no write.
    int on_batched_write_requests(dsn::message_ex **requests,
                                  int count,
                                  int64_t decree,
                                  uint64_t timestamp);

    void set_default_ttl(uint32_t ttl);

private:
    // Unmarshal the request struct from message_ex, which holds request in binary form,
    // then write into rocksdb.
    int parse_and_write_requests(dsn::message_ex **requests,
                                 int count,
                                 int64_t decree,
                                 uint64_t timestamp);

    /// Delay replying for the batched requests until all of them complete.
    int on_batched_writes(dsn::message_ex **requests, int count);

    int on_single_put_in_batch(put_rpc &rpc)
    {
        int err = _write_svc->batch_put(_write_ctx, rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request().key);
        return err;
    }

    int on_single_remove_in_batch(remove_rpc &rpc)
    {
        int err = _write_svc->batch_remove(_decree, rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request());
        return err;
    }

    // Ensure that the write request is directed to the right partition.
    // In verbose mode it will log for every request.
    void request_key_check(int64_t decree, dsn::message_ex *m, const dsn::blob &key);

private:
    friend class pegasus_server_write_test;
    friend class pegasus_write_service_test;

    std::unique_ptr<pegasus_write_service> _write_svc;
    std::vector<put_rpc> _put_rpc_batch;
    std::vector<remove_rpc> _remove_rpc_batch;

    db_write_context _write_ctx;
    int64_t _decree;

    const bool _verbose_log;
};

} // namespace server
} // namespace pegasus
