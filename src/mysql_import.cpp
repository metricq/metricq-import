// Copyright (c) 2018, ZIH,
// Technische Universitaet Dresden,
// Federal Republic of Germany
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of metricq nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <hta/hta.hpp>
#include <hta/ostream.hpp>

#include <nlohmann/json.hpp>

#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

// The header uses removed exception specification... so we must use this ugly workaround
#define throw(...)
#include <mysql_driver.h>
#undef throw

#include <boost/program_options.hpp>
#include <boost/timer/timer.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>

#include <cassert>
#include <cmath>

extern "C"
{
#include <signal.h>
}

namespace po = boost::program_options;
using json = nlohmann::json;

volatile sig_atomic_t stop_requested = 0;

void handle_signal(int)
{
    std::cerr << "caught sigint, requesting stop." << std::endl;
    stop_requested = 1;
}

json read_json_from_file(const std::filesystem::path& path)
{
    std::ifstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(path);
    json config;
    config_file >> config;
    return config;
}

struct stats
{
    uint64_t min_timestamp;
    uint64_t max_timestamp;
    uint64_t count;
};

stats stats_query(sql::Connection& db, const std::string metric)
{
    auto query =
        std::string("SELECT COUNT(`timestamp`), MIN(`timestamp`), MAX(`timestamp`) FROM ") + metric;
    auto stmt = std::unique_ptr<sql::PreparedStatement>(db.prepareStatement(query));
    auto result = std::unique_ptr<sql::ResultSet>(stmt->executeQuery());
    assert(result->next());
    stats ret;
    ret.count = result->getUInt64(1);
    ret.min_timestamp = result->getUInt64(2);
    ret.max_timestamp = result->getUInt64(3);
    return ret;
}

void import(sql::Connection& in_db, hta::Directory& out_directory,
            const std::string& in_metric_name, const std::string& out_metric_name,
            uint64_t min_timestamp, uint64_t max_timestamp, uint64_t max_limit)
{
    boost::timer::cpu_timer timer;

    auto stats = stats_query(in_db, in_metric_name);
    auto& out_metric = out_directory[out_metric_name];

    uint64_t row = 0;
    hta::TimePoint previous_time;
    uint64_t current_dataheap_timestamp;

    std::string query = std::string("SELECT timestamp, value FROM ") + in_metric_name +
                        " WHERE timestamp >= ? AND timestamp < ?" +
                        " ORDER BY timestamp ASC LIMIT ?";

    std::unique_ptr<sql::PreparedStatement> stmt(in_db.prepareStatement(query));

    min_timestamp = std::max(min_timestamp, stats.min_timestamp);
    if (max_timestamp)
    {
        max_timestamp = std::min(max_timestamp, stats.max_timestamp + 1);
    }
    else
    {
        max_timestamp = stats.max_timestamp + 1;
    }

    auto sampling_interval =
        static_cast<double>(stats.max_timestamp - stats.min_timestamp) / stats.count;
    uint64_t chunk_timedelta =
        sampling_interval * max_limit / 2; // Use 1/2 to not run into limit too often

    std::cout << "[" << out_metric_name << "] starting import from " << in_metric_name
              << " using a chunk time of " << chunk_timedelta << std::endl;

    auto current_timestamp = min_timestamp;
    while (true)
    {
        if (current_timestamp >= max_timestamp)
        {
            std::cout << "[" << out_metric_name << "] completed import of " << row << " rows\n";
            std::cout << timer.format() << std::endl;
            return;
        }

        uint64_t next_timestamp = std::min(current_timestamp + chunk_timedelta, max_timestamp);

        stmt->setUInt64(1, current_timestamp);
        stmt->setUInt64(2, next_timestamp);
        stmt->setUInt64(3, max_limit);

        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());

        if (res->rowsCount() == 0)
        {
            current_timestamp = next_timestamp;
            continue;
        }
        while (res->next())
        {
            current_dataheap_timestamp = res->getUInt64(1);
            row++;
            hta::TimePoint hta_time{ hta::duration_cast(
                std::chrono::milliseconds(current_dataheap_timestamp)) };
            if (hta_time <= previous_time)
            {
                std::cout << "Skipping non-monotonous timestamp " << hta_time << std::endl;
                continue;
            }
            previous_time = hta_time;
            out_metric.insert({ hta_time, static_cast<double>(res->getDouble(2)) });
        }

        out_metric.flush();
        std::cout << "[" << out_metric_name << "] " << row << " rows completed." << std::endl;

        current_timestamp = current_dataheap_timestamp + 1;
    }
}

int main(int argc, char* argv[])
{
    std::string config_file = "config.json";
    uint64_t min_timestamp = 0;
    uint64_t max_timestamp = 0;
    size_t chunk_size = 20000000;

    po::options_description desc("Import dataheap database into HTA");

    // clang-format off
    desc.add_options()(
        "help", "produce help message")(
        "config,c", po::value(&config_file), "path to config file (default \"config.json\").")(
        "metric,m", po::value<std::string>(), "name of metric")(
        "import-metric", po::value<std::string>(), "import name of metric")(
        "mysql-chunk-size", po::value(&chunk_size), "the chunksize for mysql streaming")(
        "min-timestamp", po::value(&min_timestamp), "minimal timestamp for dump, in unix-ms")(
        "max-timestamp", po::value(&max_timestamp), "maximal timestamp for dump, in unix-ms");
    // clang-format on

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
        std::cout << desc << "\n";
        return 0;
    };

    if (!vm.count("metric"))
    {
        std::cerr << "Error: Missing argument for import metric\n";
        std::cout << desc << "\n";
        return 1;
    }

    // for thousands separators
    std::cout.imbue(std::locale(""));

    auto out_metric_name = vm["metric"].as<std::string>();
    auto in_metric_name = out_metric_name;

    if (vm.count("import-metric"))
    {
        in_metric_name = vm["import-metric"].as<std::string>();
    }
    else
    {
        std::replace(in_metric_name.begin(), in_metric_name.end(), '.', '_');
    }
    // DO NOT do this. There are metrics like foo/bar_baz, which should be foo.bar_baz
    // std::replace(out_metric_name.begin(), out_metric_name.end(), '_', '.');

    auto config = read_json_from_file(std::filesystem::path(config_file));

    // setup input / import database
    sql::Driver* driver = sql::mysql::get_driver_instance();
    const auto& conf_import = config["import"];
    std::string host = conf_import["host"];
    std::string user = conf_import["user"];
    std::string password = conf_import["password"];
    std::string schema = conf_import["database"];
    std::unique_ptr<sql::Connection> con(driver->connect(host, user, password));
    con->setSchema(schema);

    for (auto metric_config : config["metrics"])
    {
        if (metric_config["name"] == out_metric_name)
        {
            config["metrics"] = json::array({ metric_config });
            break;
        }
    }

    hta::Directory out_directory(config);

    signal(SIGINT, handle_signal);
    import(*con, out_directory, in_metric_name, out_metric_name, min_timestamp, max_timestamp,
           chunk_size);
}
