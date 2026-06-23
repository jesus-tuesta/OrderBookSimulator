#include "barAsParquet.hpp"
#include "channel.hpp"
#include "commons.hpp"
#include "configBars.h"
#include "networking.hpp"
#include "orderBookTrack.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <string>
#include <thread>
#include <tuple>

using namespace std::chrono;
namespace asio = boost::asio;
using json     = nlohmann::json;

void alpaca_connect(WebsocketClient& client, std::ostream& log_file)
{
  client.connect();
  // Send authentication JSON
  json auth = {
    {"action", "auth"},
    {"key", config::get_api_key()},
    {"secret", config::get_api_secret()}};

  client.send(auth);
  // Read server response
  std::string response = client.readMessage();
  std::cout << "Auth response: " << response << std::endl;
  log_file << "Auth response: " << response << std::endl;

  // --- Subscribe to BTC/USD orderbook and trades ---
  json sub_msg = {{"action", "subscribe"}, {"orderbooks", {"BTC/USD"}}};

  client.send(sub_msg);

  response = client.readMessage();
  std::cout << "Sub response: " << response << std::endl;
  log_file << "Sub response: " << response << std::endl;
  response = client.readMessage();
  std::cout << "Subscriptions: " << response << std::endl;
  log_file << "Subscriptions: " << response << std::endl;
}

void process_message(
  std::string       msg,
  std::ostream&     alpaca_data_out,
  orderBook&        ob,
  std::vector<bar>& bars,
  std::ostream&     log_file,
  std::ostream&     bars_file)
{
  // this should be a timestamp from the current bar
  auto now = std::chrono::steady_clock::now();
  try {
    json parsed = json::parse(msg);
    alpaca_data_out << parsed.dump() << "\n";
    for (int i = 0; i < parsed.size(); ++i) {
      auto resp = parsed[i];
      if (resp["T"] == "o") {
        message m = resp.get<message>();
        for (auto u : m.askBook) {
          ob.receiveQuoteUpdate(u.price, Side::a, u.quantity, m.time, log_file);
        }
        for (auto u : m.bidBook) {
          ob.receiveQuoteUpdate(u.price, Side::b, u.quantity, m.time, log_file);
        }
      }
      if (resp["T"] == "t") {
        trade t = resp.get<trade>();
        ob.addUpdateTrades(t.price, t.side, t.size, t.time, log_file);
      }
    }
  }
  catch (json::parse_error& e) {
    std::cerr << "Invalid JSON received: " << msg << std::endl;
  }
  if ((now > ob.next_bar_close) || (now == ob.next_bar_close)) {
    bars.push_back(ob.createBar(bars_file));
    ob.next_bar_close += ob.bar_interval;
  }
}

std::tuple<std::ofstream, std::ofstream, std::ofstream> defining_logs()
{
  std::ofstream log_file("src/barGeneration/log.txt", std::ios::out);
  if (!log_file.is_open())
    throw std::runtime_error("Failed to open log file");

  std::ofstream bars_file("src/barGeneration/bars.txt", std::ios::out);
  if (!bars_file.is_open())
    throw std::runtime_error("Failed to open bars file");

  std::ofstream alpaca_data_out(
    "src/barGeneration/alpaca_data.jsonl", std::ios::out);
  if (!alpaca_data_out.is_open())
    throw std::runtime_error("Failed to open output file");
  return std::make_tuple(
    std::move(log_file), std::move(bars_file), std::move(alpaca_data_out));
}

void write_csv_metrics(std::vector<thread_info> metric)
{
  std::ofstream csv("metrics.csv");
  csv << "ts_ns,consumer,producer,queue_size\n";
  for (auto const& line : metric) {
    csv << line.ts.time_since_epoch().count() << "," << line.consumer_run << ","
        << line.producer_run << "," << line.queue_size << "\n";
  }
}

template<typename T>
using channel_type = std::conditional_t<
  config::thread_config == multithread_type::lock_free,
  channel_lock_free<T>,
  channel_lock_based<T>>;

int main()
{
  auto logs                                    = defining_logs();
  auto& [log_file, bars_file, alpaca_data_out] = logs;
  net::io_context ioc;

  std::vector<thread_info> metrics;
  auto                     last_dump = std::chrono::steady_clock::time_point{};
  std::vector<bar>         bars;
  try {
    WebsocketClient client(config::HOST, config::PORT, config::TARGET, ioc);
    alpaca_connect(client, log_file);

    const auto runtime = std::chrono::seconds(60);

    orderBook ob;
    ob.firstBarCameIn();

    channel_type<std::string> ch(100'000);
    std::thread               producer([&] {
      try {
        while (true) {
          std::string msg = client.readMessage();
          ch.send(std::move(msg));
        }
      }
      catch (std::exception const& e) {
        std::cerr << "Producer error: " << e.what() << std::endl;
      }
    });

    std::thread consumer([&] {
      std::string msg;
      while (ch.receive(msg)) {
        auto now = std::chrono::steady_clock::now();
        if (now - last_dump > 200ms) {
          metrics.push_back(ch.current_status());
          last_dump = now;
        }
        process_message(msg, alpaca_data_out, ob, bars, log_file, bars_file);
      }
    });

    std::this_thread::sleep_for(runtime);
    ch.close();
    client.shutdown();

    producer.join();
    consumer.join();

    write_csv_metrics(metrics);
    client.close();
    saveBarsToParquet(bars);
    std::cout << "Data collection complete, saved to alpaca_data.jsonl\n";
    log_file << "Data collection complete, saved to alpaca_data.jsonl\n";
    ob.showBook(log_file);
    ob.showTrades(log_file);
  }
  catch (std::exception const& e) {
    std::cerr << "Error: " << e.what() << std::endl;
  }
  return 0;
}
