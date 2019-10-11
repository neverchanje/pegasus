// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <dsn/dist/replication/replica_base.h>
#include <dsn/dist/replication/duplication_common.h>

#include "base/pegasus_value_schema.h"
#include "base/pegasus_utils.h"
#include "rrdb/rrdb_types.h"

namespace pegasus {
namespace server {

inline uint8_t get_current_cluster_id()
{
    static const uint8_t cluster_id =
        dsn::replication::get_duplication_cluster_id(dsn::replication::get_current_cluster_name())
            .get_value();
    return cluster_id;
}

// The context of an update to the database.
struct db_write_context
{
    int64_t decree{0};          // the mutation decree
    uint64_t timestamp{0};      // the timestamp of this write
    uint64_t remote_timetag{0}; // timetag of the remote write, 0 if it's not from remote.

    static inline db_write_context empty(int64_t d) { return create(d, 0); }

    static inline db_write_context create(int64_t decree, uint64_t timestamp)
    {
        db_write_context ctx;
        ctx.decree = decree;
        ctx.timestamp = timestamp;
        return ctx;
    }

    static inline db_write_context create_duplicate(int64_t decree, uint64_t remote_timetag)
    {
        db_write_context ctx;
        ctx.decree = decree;
        ctx.remote_timetag = remote_timetag;
        return ctx;
    }

    bool is_duplicated_write() const { return remote_timetag > 0; }
};

class pegasus_server_impl;
class capacity_unit_calculator;

/// Handle the write requests.
/// As the signatures imply, this class is not responsible for replying the rpc,
/// the caller(pegasus_server_write) should do.
/// \see pegasus::server::pegasus_server_write::on_batched_write_requests
class pegasus_write_service
{
public:
    explicit pegasus_write_service(pegasus_server_impl *server);

    ~pegasus_write_service();

    // Write empty record.
    // See this document (https://github.com/XiaoMi/pegasus/wiki/last_flushed_decree)
    // to know why we must have empty write.
    int empty_put(int64_t decree);

    // Write MULTI_PUT record.
    int multi_put(const db_write_context &ctx,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp);

    // Write MULTI_REMOVE record.
    int multi_remove(int64_t decree,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp);

    // Write INCR record.
    int incr(int64_t decree, const dsn::apps::incr_request &update, dsn::apps::incr_response &resp);

    // Write CHECK_AND_SET record.
    int check_and_set(int64_t decree,
                      const dsn::apps::check_and_set_request &update,
                      dsn::apps::check_and_set_response &resp);

    // Write CHECK_AND_MUTATE record.
    int check_and_mutate(int64_t decree,
                         const dsn::apps::check_and_mutate_request &update,
                         dsn::apps::check_and_mutate_response &resp);

    // Write PUT record duplicated from remote.
    int duplicated_put(const db_write_context &ctx,
                       const dsn::apps::update_request &update,
                       dsn::apps::update_response &resp);

    // Write MULTI_PUT record duplicated from remote.
    int duplicated_multi_put(const db_write_context &ctx,
                             const dsn::apps::multi_put_request &update,
                             dsn::apps::update_response &resp);

    // Write REMOVE record duplicated from remote.
    int duplicated_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp);

    // Write MULTI_REMOVE record duplicated from remote.
    int duplicated_multi_remove(int64_t decree,
                                const dsn::apps::multi_remove_request &update,
                                dsn::apps::multi_remove_response &resp);

    /// For batch write.
    /// NOTE: A batch write may incur a database read for consistency check of timetag.
    /// (see pegasus::pegasus_value_generator::generate_value_v1 for more info about timetag)
    /// To disable the consistency check, unset `verify_timetag` under `pegasus.server` section
    /// in configuration.

    // Prepare batch write.
    void batch_prepare(int64_t decree);

    // Add PUT record in batch write.
    // \returns 0 if success, non-0 if failure.
    // NOTE that `resp` should not be moved or freed while the batch is not committed.
    int batch_put(const db_write_context &ctx,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp);

    // Add REMOVE record in batch write.
    // \returns 0 if success, non-0 if failure.
    // NOTE that `resp` should not be moved or freed while the batch is not committed.
    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp);

    // Commit batch write.
    // \returns 0 if success, non-0 if failure.
    // NOTE that if the batch contains no updates, 0 is returned.
    int batch_commit(int64_t decree);

    // Abort batch write.
    void batch_abort(int64_t decree, int err);

    void set_default_ttl(uint32_t ttl);

private:
    void clear_up_batch_states();

private:
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;

    pegasus_server_impl *_server;

    class impl;
    std::unique_ptr<impl> _impl;

    uint64_t _batch_start_time;

    capacity_unit_calculator *_cu_calculator;

    ::dsn::perf_counter_wrapper _pfc_put_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_put_qps;
    ::dsn::perf_counter_wrapper _pfc_remove_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_qps;
    ::dsn::perf_counter_wrapper _pfc_incr_qps;
    ::dsn::perf_counter_wrapper _pfc_check_and_set_qps;
    ::dsn::perf_counter_wrapper _pfc_check_and_mutate_qps;

    ::dsn::perf_counter_wrapper _pfc_put_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_put_latency;
    ::dsn::perf_counter_wrapper _pfc_remove_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_latency;
    ::dsn::perf_counter_wrapper _pfc_incr_latency;
    ::dsn::perf_counter_wrapper _pfc_check_and_set_latency;
    ::dsn::perf_counter_wrapper _pfc_check_and_mutate_latency;

    ::dsn::perf_counter_wrapper _pfc_duplicated_put_qps;
    ::dsn::perf_counter_wrapper _pfc_duplicated_multi_put_qps;
    ::dsn::perf_counter_wrapper _pfc_duplicated_remove_qps;
    ::dsn::perf_counter_wrapper _pfc_duplicated_multi_remove_qps;

    // Records all requests.
    std::vector<::dsn::perf_counter *> _batch_qps_perfcounters;
    std::vector<::dsn::perf_counter *> _batch_latency_perfcounters;

    // TODO(wutao1): add perf counters for failed rpc.
};

} // namespace server
} // namespace pegasus
