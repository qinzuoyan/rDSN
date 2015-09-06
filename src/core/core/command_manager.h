/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

# include <dsn/internal/command.h>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/singleton.h>
# include <dsn/internal/rpc_message.h>
# include <map>

namespace dsn {

    class command_manager : public ::dsn::utils::singleton<command_manager>
    {
    public:
        command_manager();

        void register_command(const std::vector<const char*>& commands, const char* help_one_line, const char* help_long, command_handler handler);
        bool run_command(const std::string& cmdline, __out_param std::string& output);
        void run_console();
        void start_local_cli();
        void start_remote_cli();
        void on_remote_cli(dsn_message_t req);

    private:
        bool run_command(const std::string& cmd, const std::vector<std::string>& args, __out_param std::string& output);

    private:
        struct command
        {
            std::vector<const char*> commands;
            std::string     help_short;
            std::string     help_long;
            command_handler handler;
        };

        ::dsn::utils::rw_lock_nr        _lock;
        std::map<std::string, command*> _handlers;
        std::vector<command*>           _commands;
    };

}
