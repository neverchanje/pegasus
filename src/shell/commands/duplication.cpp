// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"
#include "shell/argh.h"

#include <fmt/ostream.h>
#include <dsn/utility/errors.h>
#include <dsn/utility/output_utils.h>
#include <dsn/utility/string_conv.h>
#include <dsn/dist/replication/duplication_common.h>

using dsn::replication::duplication_status;
using dsn::replication::dupid_t;

bool add_dup(command_executor *e, shell_context *sc, arguments args)
{
    // add_dup <app_name> <remote_address> [-f]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 3) {
        fmt::print(stderr, "too much params\n");
        return false;
    }
    for (const auto &flag : cmd.flags()) {
        if (flag != "f" && flag != "freeze") {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    std::string app_name;
    if (!cmd(1)) {
        fmt::print(stderr, "missing param <app_name>\n");
        return false;
    }
    app_name = cmd(1).str();

    std::string remote_address;
    if (!cmd(2)) {
        fmt::print(stderr, "missing param <remote_address>\n");
        return false;
    }
    remote_address = cmd(2).str();
    if (remote_address == sc->current_cluster_name) {
        fmt::print(stderr,
                   "illegal operation: adding duplication to itself [remote: {}]\n",
                   remote_address);
        return true;
    }

    bool freeze = cmd[{"-f", "--freeze"}];

    auto err_resp = sc->ddl_client->add_dup(app_name, remote_address, freeze);
    dsn::error_s err = err_resp.get_error();
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
    }
    if (!err.is_ok()) {
        fmt::print(
            stderr,
            "adding duplication failed [app: {}, remote address: {}, freeze: {}, error: {}]\n",
            app_name,
            remote_address,
            freeze,
            err.description());
    } else {
        const auto &resp = err_resp.get_value();
        fmt::print("Success for adding duplication [app: {}, remote address: {}, appid: {}, dupid: "
                   "{}, freeze: {}]\n",
                   app_name,
                   remote_address,
                   resp.appid,
                   resp.dupid,
                   freeze);
    }
    return true;
}

bool string2dupid(const std::string &str, dupid_t *dup_id)
{
    bool ok = dsn::buf2int32(str, *dup_id);
    if (!ok) {
        fmt::print(stderr, "parsing {} as positive int failed: {}\n", str);
        return false;
    }
    return true;
}

bool query_dup(command_executor *e, shell_context *sc, arguments args)
{
    // query_dup <app_name> [-d]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 2) {
        fmt::print(stderr, "too much params\n");
        return false;
    }
    for (const auto &flag : cmd.flags()) {
        if (flag != "d" && flag != "detail") {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    std::string app_name;
    if (!cmd(1)) {
        fmt::print(stderr, "missing param <app_name>\n");
        return false;
    }
    app_name = cmd(1).str();

    bool detail = cmd[{"-d", "--detail"}];

    auto err_resp = sc->ddl_client->query_dup(app_name);
    dsn::error_s err = err_resp.get_error();
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
    }
    if (!err.is_ok()) {
        fmt::print(stderr,
                   "querying duplications of app [{}] failed, error={}\n",
                   app_name,
                   err.description());
    } else if (detail) {
        fmt::print("duplications of app [{}] in detail:\n", app_name);
        const auto &resp = err_resp.get_value();
        for (auto info : resp.entry_list) {
            fmt::print("{}\n\n", duplication_entry_to_string(info));
        }
    } else {
        const auto &resp = err_resp.get_value();
        fmt::print("duplications of app [{}] are listed as below:\n", app_name);

        dsn::utils::table_printer printer;
        printer.add_title("dup_id");
        printer.add_column("status");
        printer.add_column("remote cluster");
        printer.add_column("create time");

        char create_time[25];
        for (auto info : resp.entry_list) {
            dsn::utils::time_ms_to_date_time(info.create_ts, create_time, sizeof(create_time));

            printer.add_row(info.dupid);
            printer.append_data(duplication_status_to_string(info.status));
            printer.append_data(info.remote);
            printer.append_data(create_time);

            printer.output(std::cout);
            std::cout << std::endl;
        }
    }
    return true;
}

bool change_dup_status(command_executor *e,
                       shell_context *sc,
                       const arguments &args,
                       duplication_status::type status)
{
    if (args.argc <= 2) {
        return false;
    }

    std::string app_name = args.argv[1];

    dupid_t dup_id;
    if (!string2dupid(args.argv[2], &dup_id)) {
        return false;
    }

    std::string operation;
    switch (status) {
    case duplication_status::DS_START:
        operation = "starting duplication";
        break;
    case duplication_status::DS_PAUSE:
        operation = "pausing duplication";
        break;
    case duplication_status::DS_REMOVED:
        operation = "removing duplication";
        break;
    default:
        dfatal("unexpected duplication status %d", status);
    }

    auto err_resp = sc->ddl_client->change_dup_status(app_name, dup_id, status);
    dsn::error_s err = err_resp.get_error();
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
    }
    if (err.is_ok()) {
        fmt::print("{}({}) for app [{}] succeed\n", operation, dup_id, app_name);
    } else {
        fmt::print(stderr,
                   "{}({}) for app [{}] failed, error={}\n",
                   operation,
                   dup_id,
                   app_name,
                   err.description());
    }
    return true;
}

bool remove_dup(command_executor *e, shell_context *sc, arguments args)
{
    return change_dup_status(e, sc, args, duplication_status::DS_REMOVED);
}

bool start_dup(command_executor *e, shell_context *sc, arguments args)
{
    return change_dup_status(e, sc, args, duplication_status::DS_START);
}

bool pause_dup(command_executor *e, shell_context *sc, arguments args)
{
    return change_dup_status(e, sc, args, duplication_status::DS_PAUSE);
}
