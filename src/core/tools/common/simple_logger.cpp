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
# include "simple_logger.h"
# include <sstream>
# include <boost/filesystem.hpp>

namespace dsn {
    namespace tools {

        static void print_header(FILE* fp)
        {
            uint64_t ts = 0;
            if (::dsn::tools::is_engine_ready())
                ts = dsn_now_ns();

            char str[24];
            ::dsn::utils::time_ms_to_string(ts/1000000, str);

            int tid = ::dsn::utils::get_current_tid(); 

            fprintf(fp, "%s (%llu %04x) ", str, static_cast<long long unsigned int>(ts), tid);

            task* t = task::get_current_task();
            if (t)
            {
                if (nullptr != task::get_current_worker())
                {
                    fprintf(fp, "%6s.%7s%u.%016llx: ",
                        task::get_current_node_name(),
                        task::get_current_worker()->pool_spec().name.c_str(),
                        task::get_current_worker()->index(),
                        static_cast<long long unsigned int>(t->id())
                        );
                }
                else
                {
                    fprintf(fp, "%6s.%7s.%05d.%016llx: ",
                        task::get_current_node_name(),
                        "io-thrd",
                        tid,
                        static_cast<long long unsigned int>(t->id())
                        );
                }
            }
            else
            {
                fprintf(fp, "%6s.%7s.%05d: ",
                    task::get_current_node_name(),
                    "io-thrd",
                    tid
                    );
            }
        }

        void screen_logger::dsn_logv(const char *file,
            const char *function,
            const int line,
            dsn_log_level_t log_level,
            const char* title,
            const char *fmt,
            va_list args
            )
        {
            utils::auto_lock<::dsn::utils::ex_lock_nr> l(_lock);

            print_header(stdout);
            // printf("%s:%d:%s(): ", title, line, function);
            vprintf(fmt, args);
            printf("\n");
        }

        void screen_logger::flush()
        {
            ::fflush(stdout);
        }

        simple_logger::simple_logger() 
        {
            _start_index = 0;
            _index = 0;
            _lines = 0;
            _log = nullptr;
            _short_header = dsn_config_get_value_bool("tools.simple_logger", "short_header", 
                false, "whether to use short header (excluding file/function etc.)");

            // check existing log files
            boost::filesystem::directory_iterator endtr;
            for (boost::filesystem::directory_iterator it(std::string("./"));
                it != endtr;
                ++it)
            {
                auto name = it->path().filename().string();
                if (name.length() <= 8 ||
                    name.substr(0, 4) != "log.")
                    continue;

                int index;
                if (1 != sscanf(name.c_str(), "log.%d.txt", &index))
                    continue;

                if (index > _index)
                    _index = index;

                if (_start_index == 0 || index < _start_index)
                    _start_index = index;
            }

            if (_start_index == 0)
                _start_index = _index;

            create_log_file();
        }

        void simple_logger::create_log_file()
        {
            if (_log != nullptr)
                fclose(_log);

            _lines = 0;

            std::stringstream str;
            str << "log." << ++_index << ".txt";
            _log = fopen(str.str().c_str(), "w+");  

            // TODO: move gc out of criticial path
            if (_index - _start_index > 20)
            {
                std::stringstream str2;
                str2 << "log." << _start_index++ << ".txt";
                boost::filesystem::path dp = str2.str();
                if (::dsn::utils::is_file_or_dir_exist(str2.str().c_str()))
                    boost::filesystem::remove(dp);
            }
        }

        simple_logger::~simple_logger(void) 
        { 
            fclose(_log);
        }

        void simple_logger::flush()
        {
            ::fflush(_log);
        }

        void simple_logger::dsn_logv(const char *file,
            const char *function,
            const int line,
            dsn_log_level_t log_level,
            const char* title,
            const char *fmt,
            va_list args
            )
        {
            va_list args2;
            if (log_level >= LOG_LEVEL_WARNING)
            {
                va_copy(args2, args);
            }

            utils::auto_lock<::dsn::utils::ex_lock_nr> l(_lock);
         
            print_header(_log);
            if (!_short_header)
            {
                fprintf(_log, "%s:%d:%s(): ", title, line, function);
            }            
            vfprintf(_log, fmt, args);
            fprintf(_log, "\n");
            if (log_level >= LOG_LEVEL_ERROR)
                fflush(_log);

            if (log_level >= LOG_LEVEL_WARNING)
            {
                print_header(stdout);
                if (!_short_header)
                {
                    printf("%s:%d:%s(): ", title, line, function);
                }
                vprintf(fmt, args2);
                printf("\n");
            }

            if (++_lines >= 200000)
                create_log_file();
        }
    }
}
