#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <coroutine>
#include <nlohmann/json.hpp>

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = net::ssl;
using tcp           = net::ip::tcp;
using json          = nlohmann::json;

class WebsocketClient {
public:
  WebsocketClient(
    std::string_view host,
    std::string_view port,
    std::string_view target,
    net::io_context& ioc);
  void                        connect();
  void                        send(const json& j);
  std::string                 readMessage();
  net::awaitable<std::string> async_readMessage();
  void                        close();

private:
  void load_root_certificates(ssl::context& ctx);

  ssl::context                                      ctx;
  tcp::resolver                                     resolver;
  websocket::stream<beast::ssl_stream<tcp::socket>> ws;
  beast::flat_buffer                                buffer;

  std::string host, port, target;
};

WebsocketClient::WebsocketClient(
  std::string_view host_,
  std::string_view port_,
  std::string_view target_,
  net::io_context& ioc)
  : host(host_)
  , port(port_)
  , target(target_)
  , ctx(ssl::context::tlsv12_client)
  , resolver(ioc)
  , ws(ioc, ctx)
{
}

void WebsocketClient::load_root_certificates(ssl::context& ctx)
{
  ctx.set_default_verify_paths();
}

void WebsocketClient::connect()
{
  load_root_certificates(ctx);

  auto results = resolver.resolve(host, port);
  net::connect(beast::get_lowest_layer(ws), results);

  if (!SSL_set_tlsext_host_name(
        ws.next_layer().native_handle(), host.c_str())) {
    throw beast::system_error(
      static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
  }

  ws.next_layer().handshake(ssl::stream_base::client);
  ws.handshake(host, target);
}

void WebsocketClient::send(const json& j)
{
  ws.write(net::buffer(j.dump()));
}

std::string WebsocketClient::readMessage()
{
  // The correct syntax when handeling buffer is
  // ws.read(buffer) ads the latest message into the buffer
  // beast::make_printable(buffer.data()) throws the info in bits somewhere
  // buffer.consume(buffer.size()) cleans the buffer so the next read is only
  // one line if you don't do this, the next we.read(buffer) will append and
  // you'll have two messages together
  buffer.consume(buffer.size());
  ws.read(buffer);
  return beast::buffers_to_string(buffer.data());
}

net::awaitable<std::string> WebsocketClient::async_readMessage()
{
  buffer.consume(buffer.size());
  co_await ws.async_read(buffer, net::use_awaitable);
  co_return beast::buffers_to_string(buffer.data());
}

void WebsocketClient::close()
{
  beast::error_code ec;
  ws.close(websocket::close_code::normal, ec);
}
